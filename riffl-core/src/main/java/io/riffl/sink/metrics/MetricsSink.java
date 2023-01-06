package io.riffl.sink.metrics;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsSink extends RichSinkFunction<Metric> implements CheckpointedFunction {

  private static final Logger logger = LoggerFactory.getLogger(MetricsSink.class);

  private final Metrics metrics;
  private final MetricsStore metricsStore;

  public MetricsSink(MetricsStore metricsStore) {
    this.metricsStore = metricsStore;
    this.metrics = new Metrics();
  }

  @Override
  public void invoke(Metric metric, Context context) {
    metrics.add(metric.getKey(), metric.getValue());
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    metricsStore.writeMetrics(context.getCheckpointId(), metrics);
    metrics.clear();
    checkpointedState.clear();
    checkpointedState.add(context.getCheckpointId());
  }

  private transient ListState<Long> checkpointedState;

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    var checkpointId = context.getRestoredCheckpointId().orElse(0L);
    logger.info(
        "Initialize sink state from checkpoint, {}: {}, isRestored {}",
        checkpointId,
        metrics.entrySet(),
        context.isRestored());

    ListStateDescriptor<Long> descriptor =
        new ListStateDescriptor<>("sink-checkpointId", TypeInformation.of(new TypeHint<>() {}));

    checkpointedState = context.getOperatorStateStore().getListState(descriptor);

    if (context.isRestored() && checkpointId > 1) {
      var loadedMetrics = metricsStore.loadMetrics(checkpointId - 1);
      for (var metric : loadedMetrics.entrySet()) {
        metrics.add(metric.getKey(), metric.getValue());
      }
      logger.info(
          "Initialized sink metrics from checkpoint, {}: {}", checkpointId, metrics.entrySet());
    }
  }
}
