package io.riffl.sink.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.riffl.config.Distribution;
import io.riffl.config.Sink;
import io.riffl.sink.row.RowKey;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MetricsSinkTests {

  @Test
  void metricsSinkShouldPersistMetricsIntoFile(@TempDir Path tempDir) {
    var sink = new Sink("", "table_name", "", new Distribution("Test", new Properties()), 1);

    Row row = Row.withNames(RowKind.INSERT);
    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);
    var key1 = new RowKey(row, List.of("aaa", "bbb", "ccc"));
    var key2 = new RowKey(row, List.of("aaa", "bbb"));
    var metricsSink = new MetricsSink(sink, new org.apache.flink.core.fs.Path(tempDir.toUri()));
    metricsSink.invoke(new Metric(key1, 1L), null);
    metricsSink.invoke(new Metric(key2, 10000L), null);

    var context = mock(FunctionSnapshotContext.class);
    when(context.getCheckpointId()).thenReturn(2L);

    RuntimeContext mockRuntimeContext = mock(RuntimeContext.class);
    var jobId = JobID.generate();
    when(mockRuntimeContext.getJobId()).thenReturn(jobId);
    metricsSink.setRuntimeContext(mockRuntimeContext);

    metricsSink.snapshotState(context);

    var outputFile = tempDir + "/" + jobId + "/metrics-table_name-2";
    assertTrue(Files.exists(Path.of(outputFile)));

    Map<RowKey, Long> result;
    try (FileInputStream fileInputStream = new FileInputStream(outputFile);
        ObjectInputStream objectInput = new ObjectInputStream(fileInputStream)) {
      var metrics = (Metrics) objectInput.readObject();
      result =
          metrics.entrySet().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    assertEquals(2, result.size());
    assertEquals(1L, result.get(key1));
    assertEquals(10000L, result.get(key2));
  }
}
