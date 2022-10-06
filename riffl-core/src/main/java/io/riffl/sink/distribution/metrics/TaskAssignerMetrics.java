package io.riffl.sink.distribution.metrics;

import java.util.List;
import java.util.Map;

public interface TaskAssignerMetrics {

  void add(List<Object> key, Long value);

  Map<List<Object>, Long> getMetrics();

  void clear(long checkpointId);
}
