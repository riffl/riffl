package io.riffl.sink;

import io.riffl.config.Sink;
import io.riffl.sink.allocation.TaskAllocation;
import io.riffl.sink.metrics.DefaultMetricsProcessor;
import io.riffl.sink.metrics.Metric;
import io.riffl.sink.metrics.Metrics;
import io.riffl.sink.metrics.MetricsProcessor;
import io.riffl.sink.row.TasksAssignment;
import io.riffl.sink.row.tasks.TaskAssigner;
import io.riffl.sink.row.tasks.TaskAssignerMetricsFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class PartitionerMetrics extends ProcessFunction<Row, Tuple2<Row, Integer>>
    implements CheckpointedFunction {

  private final Sink sink;
  private final Path metricsPathBase;
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
      Path metricsPathBase) {
    this.sink = sink;
    this.taskAssignerFactory = taskAssignerFactory;
    this.taskAllocation = taskAllocation;
    this.metricsPathBase = metricsPathBase;
    this.outputTag = SinkUtils.getMetricsOutputTag(sink);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    var metricsPath =
        SinkUtils.getMetricsPath(metricsPathBase, sink, getRuntimeContext().getJobId());
    this.metricsProcessor = new DefaultMetricsProcessor(sink, metricsPath);
    this.taskAllocation.configure();
    this.tasksAssignment = new TasksAssignment();
    this.taskAssigner =
        taskAssignerFactory.create(
            sink, taskAllocation.getSinkTasks(sink), tasksAssignment, new Metrics());
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
    if (context.isRestored()) {
      this.taskAssigner =
          taskAssignerFactory.create(
              sink,
              taskAllocation.getSinkTasks(sink),
              tasksAssignment,
              metricsProcessor.getMetrics(context.getRestoredCheckpointId().orElse(0)));
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
