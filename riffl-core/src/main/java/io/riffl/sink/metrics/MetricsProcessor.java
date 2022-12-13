package io.riffl.sink.metrics;

import io.riffl.sink.row.RowKey;
import java.util.function.Consumer;
import org.apache.flink.types.Row;

public interface MetricsProcessor {

  void addMetric(RowKey key, Row row);

  void flushMetrics(long checkpointId);

  boolean hasMetricsToWrite();

  void consumeMetrics(Consumer<Metric> consumer);

  Metrics getMetrics(long checkpointId);
}
