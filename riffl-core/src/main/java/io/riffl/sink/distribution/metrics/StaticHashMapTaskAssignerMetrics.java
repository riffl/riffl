package io.riffl.sink.distribution.metrics;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticHashMapTaskAssignerMetrics implements TaskAssignerMetrics, Serializable {

  private static final Logger logger =
      LoggerFactory.getLogger(StaticHashMapTaskAssignerMetrics.class);

  private static final Map<Long, Map<List<Object>, Long>> metrics =
      new ConcurrentHashMap<>(
          Map.of(-1L, new ConcurrentHashMap<>(), 0L, new ConcurrentHashMap<>()));

  private static final AtomicLong complete = new AtomicLong(-1L);
  private static final AtomicLong current = new AtomicLong(0L);

  @Override
  public void add(List<Object> key, Long value) {
    metrics.get(current.get()).merge(key, value, Long::sum);
  }

  @Override
  public Map<List<Object>, Long> getMetrics() {
    logger.debug("Metrics {}, first {}, last {}", metrics, complete, current);
    return metrics.get(complete.get());
  }

  @Override
  public void clear(long checkpointId) {
    synchronized (metrics) {
      var keySet = metrics.keySet().stream().sorted().collect(Collectors.toList());

      if (keySet.get(keySet.size() - 1) < checkpointId) {
        metrics.put(checkpointId, new ConcurrentHashMap<>());
        var purge = keySet.remove(0);
        logger.debug("Adding checkpointId store {}", checkpointId);

        current.set(checkpointId);
        complete.set(keySet.get(0));
        metrics.remove(purge);
      }
    }
  }
}
