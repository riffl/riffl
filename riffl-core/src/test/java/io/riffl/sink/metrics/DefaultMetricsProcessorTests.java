package io.riffl.sink.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.riffl.config.Distribution;
import io.riffl.config.Sink;
import io.riffl.sink.row.RowKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

public class DefaultMetricsProcessorTests {

  @Test
  void processorShouldRecordAndExposeMetricsForConsumption() {
    var sink = new Sink("", "", "", new Distribution("Test", new Properties()), 1);
    var processor = new DefaultMetricsProcessor(sink, new Path("file://"));
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
  void processorShouldLoadMetricsFromFile() {
    var sink = new Sink("", "", "", new Distribution("Test", new Properties()), 1);
    var processor =
        new DefaultMetricsProcessor(sink, new Path("./src/test/resources/metrics-table_name-"));
    var metrics = processor.getMetrics(3);
    var result =
        metrics.entrySet().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    Row row = Row.withNames(RowKind.INSERT);
    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);
    var key1 = new RowKey(row, List.of("aaa", "bbb", "ccc"));
    var key2 = new RowKey(row, List.of("aaa", "bbb"));

    assertEquals(2, result.size());
    assertEquals(1L, result.get(key1));
    assertEquals(10000L, result.get(key2));
  }
}
