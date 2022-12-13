package io.riffl.sink.metrics;

import io.riffl.config.Sink;
import io.riffl.sink.SinkUtils;
import java.io.IOException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsSink extends RichSinkFunction<Metric> implements CheckpointedFunction {

  private static final Logger logger = LoggerFactory.getLogger(MetricsSink.class);
  private final Path metricsPathBase;
  private final Metrics metrics;
  private final Sink sink;

  public MetricsSink(Sink sink, Path metricsPathBase) {
    this.sink = sink;
    this.metricsPathBase = metricsPathBase;
    this.metrics = new Metrics();
  }

  @Override
  public void invoke(Metric metric, Context context) {
    metrics.add(metric.getKey(), metric.getValue());
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    try {
      var checkpointId = context.getCheckpointId();
      var metricsPath =
          new Path(
              SinkUtils.getMetricsPath(metricsPathBase, sink, getRuntimeContext().getJobId())
                      .toString()
                  + checkpointId);
      FileSystem fs = FileSystem.getUnguardedFileSystem(metricsPath.toUri());
      try (var file = fs.create(metricsPath, WriteMode.OVERWRITE)) {
        file.write(metrics.toByteArray());
      }
      logger.info(
          "Persisted metrics for checkpoint: {}, sink {}: {}",
          checkpointId,
          sink.getTable(),
          metrics.entrySet());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    metrics.clear();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {}
}
