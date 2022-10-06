package io.riffl.sink.distribution;

import io.riffl.config.Sink;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowDistributionFunction extends RichMapFunction<Row, Tuple2<Row, Integer>>
    implements CheckpointedFunction, CheckpointListener {

  private static final Logger logger = LoggerFactory.getLogger(RebalanceTaskAssigner.class);
  private final TaskAssigner taskAssigner;
  private final Sink sink;
  private final TaskAllocation taskAllocation;

  public RowDistributionFunction(
      Sink sink, TaskAssigner taskAssigner, TaskAllocation taskAllocation) {
    this.taskAssigner = taskAssigner;
    this.sink = sink;
    this.taskAllocation = taskAllocation;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    taskAllocation.configure();
    logger.info("Parallelism {}", getRuntimeContext().getNumberOfParallelSubtasks());
    taskAssigner.configure(sink, taskAllocation.getSinkTasks(sink));
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    if (taskAssigner instanceof CheckpointedFunction) {
      ((CheckpointedFunction) taskAssigner).snapshotState(context);
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    if (taskAssigner instanceof CheckpointedFunction) {
      ((CheckpointedFunction) taskAssigner).initializeState(context);
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    if (taskAssigner instanceof CheckpointListener) {
      ((CheckpointListener) taskAssigner).notifyCheckpointComplete(checkpointId);
    }
  }

  @Override
  public Tuple2<Row, Integer> map(Row value) throws Exception {
    return new Tuple2<>(value, taskAssigner.taskIndex(value));
  }
}
