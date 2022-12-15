package io.riffl.sink;

import io.riffl.config.Sink;
import io.riffl.sink.metrics.Metric;
import java.text.MessageFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.OutputTag;

public class SinkUtils {

  public static OutputTag<Metric> getMetricsOutputTag(Sink sink) {
    return new OutputTag<>(MessageFormat.format("{0}-{1}", sink.getTable(), "metrics-sink")) {};
  }

  public static Path getMetricsPath(Path base, Sink sink) {
    return new Path(
        MessageFormat.format(
            "{0}/{1}/metrics-{2}-", base.toString(), "riffl-metrics-store", sink.getTable()));
  }

  public static String getOperatorName(Sink sink, String name) {
    return MessageFormat.format("{0}/{1}", sink.getTable(), name);
  }
}
