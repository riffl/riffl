package io.riffl.sink.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.riffl.sink.row.RowKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

public class MetricsTests {

  @Test
  void metricsShouldReturnContentAsByteArray() throws IOException, ClassNotFoundException {
    var metrics = new Metrics();
    Row row = Row.withNames(RowKind.INSERT);
    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);
    var key1 = new RowKey(row, List.of("aaa", "bbb", "ccc"));
    metrics.add(key1, 10000L);

    try (var byteArrayInput = new ByteArrayInputStream(metrics.toByteArray());
        var objectInput = new ObjectInputStream(byteArrayInput)) {
      var metricsObject = (Metrics) objectInput.readObject();
      assertEquals(metrics, metricsObject);
    }
  }
}
