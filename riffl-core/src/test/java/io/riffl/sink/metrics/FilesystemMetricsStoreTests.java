package io.riffl.sink.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.riffl.sink.row.RowKey;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FilesystemMetricsStoreTests {

  private Row getTestRow() {
    Row row = Row.withNames(RowKind.INSERT);
    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);

    return row;
  }

  @Test
  void metricsShouldBeLoadedFromFile() {
    var metricsStore =
        new FilesystemMetricsStore(new Path("./src/test/resources/metrics-table_name-"));

    var metrics = metricsStore.loadMetrics(2);
    var result =
        metrics.entrySet().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    var key1 = new RowKey(getTestRow(), List.of("aaa", "bbb", "ccc"));
    var key2 = new RowKey(getTestRow(), List.of("aaa", "bbb"));

    assertEquals(2, result.size());
    assertEquals(1L, result.get(key1));
    assertEquals(10000L, result.get(key2));
  }

  @Test
  void metricsShouldBeWrittenAndOverwrittenIntoFile(@TempDir java.nio.file.Path tempDir) {
    var tempPrefix = tempDir + "/metrics-";
    var metricsStore = new FilesystemMetricsStore(new Path(tempPrefix));

    var key1 = new RowKey(getTestRow(), List.of("aaa", "bbb", "ccc"));
    var key2 = new RowKey(getTestRow(), List.of("aaa", "bbb"));
    var metrics = new Metrics();
    metrics.add(key1, 1L);
    metrics.add(key2, 10000L);

    // write first time
    metricsStore.writeMetrics(2, metrics);
    var outputFile = tempPrefix + 2;
    assertTrue(Files.exists(java.nio.file.Path.of(outputFile)));

    // write second time
    metricsStore.writeMetrics(2, metrics);

    Map<RowKey, Long> result;
    try (FileInputStream fileInputStream = new FileInputStream(outputFile);
        ObjectInputStream objectInput = new ObjectInputStream(fileInputStream)) {
      var loadedMetrics = (Metrics) objectInput.readObject();
      result =
          loadedMetrics.entrySet().stream()
              .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    assertEquals(2, result.size());
    assertEquals(1L, result.get(key1));
    assertEquals(10000L, result.get(key2));
  }

  @Test
  void metricsShouldBeRemoved(@TempDir java.nio.file.Path tempDir) {
    var tempPrefix = tempDir + "/metrics-";
    var metricsStore = new FilesystemMetricsStore(new Path(tempPrefix));

    var key1 = new RowKey(getTestRow(), List.of("aaa", "bbb", "ccc"));
    var key2 = new RowKey(getTestRow(), List.of("aaa", "bbb"));
    var metrics = new Metrics();
    metrics.add(key1, 1L);
    metrics.add(key2, 10000L);

    metricsStore.writeMetrics(2, metrics);
    var outputFile = tempPrefix + 2;
    assertTrue(Files.exists(java.nio.file.Path.of(outputFile)));
    metricsStore.removeMetrics(2);
    assertFalse(Files.exists(java.nio.file.Path.of(outputFile)));
  }
}
