package io.riffl.sink;

import io.riffl.config.Sink;
import io.riffl.sink.allocation.TaskAllocation;
import io.riffl.sink.metrics.DefaultMetricsProcessor;
import io.riffl.sink.metrics.Metric;
import io.riffl.sink.metrics.Metrics;
import io.riffl.sink.metrics.MetricsProcessor;
import io.riffl.sink.metrics.MetricsStore;
import io.riffl.sink.row.TasksAssignment;
import io.riffl.sink.row.tasks.TaskAssigner;
import io.riffl.sink.row.tasks.TaskAssignerMetricsFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionerMetrics extends ProcessFunction<Row, Tuple2<Row, Integer>>
    implements CheckpointedFunction {

  private static final Logger logger = LoggerFactory.getLogger(PartitionerMetrics.class);

  private final Sink sink;
  private final MetricsStore metricsStore;
  private final OutputTag<Metric> outputTag;
  private final TaskAssignerMetricsFactory taskAssignerFactory;
  private final TaskAllocation taskAllocation;
  private MetricsProcessor metricsProcessor;
  private TasksAssignment tasksAssignment;

  private TaskAssigner taskAssigner;

  public PartitionerMetrics(
      Sink sink,
      TaskAssignerMetricsFactory taskAssignerFactory,
      TaskAllocation taskAllocation,
      MetricsStore metricsStore) {
    this.sink = sink;
    this.taskAssignerFactory = taskAssignerFactory;
    this.taskAllocation = taskAllocation;
    this.metricsStore = metricsStore;
    this.outputTag = SinkUtils.getMetricsOutputTag(sink);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    var checkpointId = context.getCheckpointId();
    metricsProcessor.flushMetrics(checkpointId);
    this.taskAssigner =
        taskAssignerFactory.create(
            sink,
            taskAllocation.getSinkTasks(sink),
            tasksAssignment,
            metricsProcessor.getMetrics(checkpointId));
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    this.taskAllocation.configure();
    this.tasksAssignment = new TasksAssignment();
    this.metricsProcessor = new DefaultMetricsProcessor(sink, metricsStore);
    if (context.isRestored()) {
      logger.info("context.getRestoredCheckpointId():" + context.getRestoredCheckpointId());
      this.taskAssigner =
          taskAssignerFactory.create(
              sink,
              taskAllocation.getSinkTasks(sink),
              tasksAssignment,
              metricsProcessor.getMetrics(context.getRestoredCheckpointId().orElse(0)));
    } else {
      this.taskAssigner =
          taskAssignerFactory.create(
              sink, taskAllocation.getSinkTasks(sink), tasksAssignment, new Metrics());
    }
  }

  @Override
  public void processElement(
      Row row,
      ProcessFunction<Row, Tuple2<Row, Integer>>.Context ctx,
      Collector<Tuple2<Row, Integer>> out) {

    if (metricsProcessor.hasMetricsToWrite()) {
      metricsProcessor.consumeMetrics(metric -> ctx.output(outputTag, metric));
    }

    var key = taskAssigner.getKey(row);

    metricsProcessor.addMetric(key, row);

    out.collect(new Tuple2<>(row, taskAssigner.getTask(row, key)));
  }
}
