package io.riffl.sink.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class StaticHashMapTaskAssignerMetricsTests {

  @Test
  void metricsShouldBePurgedWhenOld() {
    var metrics = new StaticHashMapTaskAssignerMetrics();
    List<Object> key = Collections.singletonList("aaa");
    metrics.add(key, 1L);
    assertNull(metrics.getMetrics().get(key));

    metrics.clear(123);
    assertEquals(1, metrics.getMetrics().get(key));
    metrics.add(key, 1L);
    metrics.add(key, 1L);
    metrics.clear(123);
    assertEquals(1, metrics.getMetrics().get(key));

    metrics.clear(124);
    assertEquals(2, metrics.getMetrics().get(key));
    metrics.clear(124);
    metrics.clear(124);
    assertEquals(2, metrics.getMetrics().get(key));
    metrics.clear(125);
    assertNull(metrics.getMetrics().get(key));
  }
}
