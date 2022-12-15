package io.riffl.sink.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.riffl.config.Distribution;
import io.riffl.config.Sink;
import io.riffl.sink.row.RowKey;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

public class DefaultMetricsProcessorTests {

  @Test
  void processorShouldRecordAndExposeMetricsForConsumption() {
    var sink = new Sink("", "", "", new Distribution("Test", new Properties()), 1);
    var processor = new DefaultMetricsProcessor(sink, null);
    Row row = Row.withNames(RowKind.INSERT);

    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);
    var key = new RowKey(row, List.of("aaa", "bbb", "ccc"));
    processor.addMetric(key, row);
    processor.flushMetrics(0);
    assertTrue(processor.hasMetricsToWrite());
    var metricContainer = new HashMap<RowKey, Long>();
    processor.consumeMetrics(metric -> metricContainer.put(metric.getKey(), metric.getValue()));
    assertEquals(metricContainer.get(key), 1);
  }

  @Test
  void processorShouldLoadMetrics() {
    var sink = new Sink("", "", "", new Distribution("Test", new Properties()), 1);
    var metricStore = mock(FilesystemMetricsStore.class);
    var processor = new DefaultMetricsProcessor(sink, metricStore);
    processor.getMetrics(10);

    verify(metricStore, times(1)).loadMetrics(9L);
    verify(metricStore, times(1)).removeMetrics(5L);
  }
}
