package io.riffl.sink;

import io.riffl.config.ConfigBase;
import io.riffl.config.Sink;
import io.riffl.sink.allocation.StackedTaskAllocation;
import io.riffl.sink.allocation.TaskAllocation;
import io.riffl.sink.metrics.FilesystemMetricsStore;
import io.riffl.sink.metrics.MetricsSink;
import io.riffl.sink.row.tasks.TaskAssignerDefaultFactory;
import io.riffl.sink.row.tasks.TaskAssignerFactory;
import io.riffl.sink.row.tasks.TaskAssignerLoader;
import io.riffl.sink.row.tasks.TaskAssignerMetricsFactory;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkStream {

  private static final Logger logger = LoggerFactory.getLogger(SinkStream.class);
  private final StreamExecutionEnvironment env;
  private final StreamTableEnvironment tableEnv;

  public SinkStream(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
    this.env = env;
    this.tableEnv = tableEnv;
    logger.info(this.tableEnv.getCurrentCatalog());
  }

  private DataStream<Row> repartition(
      DataStream<Row> stream, Sink sink, TaskAllocation taskAllocation, ConfigBase appConfig) {
    var loader = new TaskAssignerLoader<>(TaskAssignerFactory.class);
    TaskAssignerFactory taskAssignerFactory = loader.load(sink.getDistribution().getClassName());
    SingleOutputStreamOperator<Tuple2<Row, Integer>> distribution;
    if (taskAssignerFactory instanceof TaskAssignerMetricsFactory) {
      var metricsConfig = appConfig.getMetrics();
      var metricsPath = SinkUtils.getMetricsPath(new Path(metricsConfig.getStoreUri()), sink);
      var metricsStore = new FilesystemMetricsStore(metricsPath, metricsConfig.getSkipOnFailure());
      var partitioner =
          new PartitionerMetrics(
              sink, (TaskAssignerMetricsFactory) taskAssignerFactory, taskAllocation, metricsStore);

      distribution =
          stream
              .process(partitioner)
              .returns(Types.TUPLE(stream.getType(), TypeInformation.of(Integer.class)))
              .name(SinkUtils.getOperatorName(sink, "partitioner"))
              .uid(SinkUtils.getOperatorName(sink, "partitioner"));

      var metricsStream = distribution.getSideOutput(SinkUtils.getMetricsOutputTag(sink));
      metricsStream
          .addSink(new MetricsSink(metricsStore))
          .name(SinkUtils.getOperatorName(sink, "metrics-sink"))
          .uid(SinkUtils.getOperatorName(sink, "metrics-sink"))
          .setParallelism(1);

    } else {
      Partitioner partitioner =
          new Partitioner(sink, (TaskAssignerDefaultFactory) taskAssignerFactory, taskAllocation);
      distribution =
          stream
              .process(partitioner)
              .returns(Types.TUPLE(stream.getType(), TypeInformation.of(Integer.class)))
              .name(SinkUtils.getOperatorName(sink, "partitioner"))
              .uid(SinkUtils.getOperatorName(sink, "partitioner"));
    }

    return distribution
        .partitionCustom(
            (org.apache.flink.api.common.functions.Partitioner<Integer>)
                (key, numPartitions) -> key,
            (KeySelector<Tuple2<Row, Integer>, Integer>) k -> k.f1)
        .map(t -> t.f0)
        .returns(stream.getType());
  }

  public StreamStatementSet build(ConfigBase appConfig) {
    var sinks = appConfig.getSinks();
    TaskAllocation taskAllocation = new StackedTaskAllocation(sinks, env.getParallelism());
    // Query
    Map<String, DataStream<Row>> queryStream =
        sinks.stream()
            .map(
                sink -> {
                  if (sink.hasCreate()) {
                    tableEnv.executeSql(sink.getCreate());
                  }

                  Table query = tableEnv.sqlQuery(sink.getQuery());

                  if (sink.hasDistribution()) {
                    return Map.entry(
                        sink.getTable(),
                        repartition(tableEnv.toDataStream(query), sink, taskAllocation, appConfig));
                  } else {
                    return Map.entry(sink.getTable(), tableEnv.toDataStream(query));
                  }
                })
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    // Sink
    StreamStatementSet set = tableEnv.createStatementSet();
    queryStream.forEach(
        (table, value) -> {
          logger.info(table);
          set.addInsert(table, tableEnv.fromDataStream(value));
        });

    return set;
  }
}
