package io.riffl.sink.metrics;

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
  public void snapshotState(FunctionSnapshotContext context) {
    metricsStore.writeMetrics(context.getCheckpointId(), metrics);
    metrics.clear();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {}
}
