package io.riffl.sink.metrics;

public interface MetricsStore {

  Metrics loadMetrics(long checkpointId);

  void removeMetrics(long checkpointId);

  void writeMetrics(long checkpointId, Metrics metrics);
}
