package io.riffl.sink;

import io.riffl.config.ConfigUtils;
import io.riffl.config.Sink;
import io.riffl.sink.allocation.StackedTaskAllocation;
import io.riffl.sink.allocation.TaskAllocation;
import io.riffl.sink.row.TaskAssigner;
import io.riffl.sink.row.TaskAssignerFactory;
import io.riffl.utils.TableHelper;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
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

  private DataStream<Row> repartition(DataStream<Row> stream, RowDistributionFunction partitioner) {

    return stream
        .map(partitioner)
        .returns(Types.TUPLE(stream.getType(), TypeInformation.of(Integer.class)))
        .partitionCustom(
            (Partitioner<Integer>) (key, numPartitions) -> key,
            (KeySelector<Tuple2<Row, Integer>, Integer>) k -> k.f1)
        .map(t -> t.f0)
        .returns(stream.getType());
  }

  public StreamStatementSet build(List<Sink> sinks, Properties overrides) {
    Map<String, TaskAssigner> taskAssigners = createTaskAssigners(sinks);

    TaskAllocation taskAllocation = new StackedTaskAllocation(sinks, env.getParallelism());
    // Query
    Map<Sink, DataStream<Row>> queryStream =
        sinks.stream()
            .map(
                sink -> {
                  Table query =
                      tableEnv.sqlQuery(
                          ConfigUtils.openFileAsString(new Path(sink.getQueryUri()), overrides));

                  if (sink.hasDistribution()) {
                    TaskAssigner taskAssigner =
                        taskAssigners.get(sink.getDistribution().getClassName());
                    RowDistributionFunction partitioner =
                        new RowDistributionFunction(sink, taskAssigner, taskAllocation);
                    return Map.entry(sink, repartition(tableEnv.toDataStream(query), partitioner));
                  } else {
                    return Map.entry(sink, tableEnv.toDataStream(query));
                  }
                })
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    // Sink
    StreamStatementSet set = tableEnv.createStatementSet();
    queryStream.forEach(
        (key, value) -> {
          Path definitionPath = new Path(key.getCreateUri());
          tableEnv.executeSql(ConfigUtils.openFileAsString(definitionPath, overrides));

          ObjectIdentifier sinkId =
              TableHelper.getCreateTableIdentifier(definitionPath, env, tableEnv);

          logger.info(sinkId.asSummaryString());

          set.addInsert(sinkId.asSummaryString(), tableEnv.fromDataStream(value));
        });

    return set;
  }

  private Map<String, TaskAssigner> createTaskAssigners(List<Sink> sinks) {
    return sinks.stream()
        .filter(s -> s.getDistribution() != null)
        .map(s -> s.getDistribution().getClassName())
        .distinct()
        .map(className -> Map.entry(className, TaskAssignerFactory.load(className)))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }
}
