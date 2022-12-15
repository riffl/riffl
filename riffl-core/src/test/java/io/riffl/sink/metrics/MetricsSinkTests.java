package io.riffl.sink.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.riffl.sink.row.RowKey;
import java.util.List;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

public class MetricsSinkTests {

  private Row getTestRow() {
    Row row = Row.withNames(RowKind.INSERT);
    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);

    return row;
  }

  @Test
  void metricsSinkShouldPersistMetricsWithMetricsStore() {

    var key1 = new RowKey(getTestRow(), List.of("aaa", "bbb", "ccc"));
    var key2 = new RowKey(getTestRow(), List.of("aaa", "bbb"));

    var context = mock(FunctionSnapshotContext.class);
    when(context.getCheckpointId()).thenReturn(2L);

    var metricsStore = mock(MetricsStore.class);
    var metricsSink = new MetricsSink(metricsStore);
    metricsSink.invoke(new Metric(key1, 1L), null);
    metricsSink.invoke(new Metric(key2, 10000L), null);
    var resultMetrics = new Metrics();
    resultMetrics.add(key1, 1L);
    resultMetrics.add(key2, 10000L);

    Answer<Metrics> answer =
        invocation -> {
          assertEquals(resultMetrics, invocation.getArgument(1, Metrics.class));
          return invocation.getArgument(1);
        };

    doAnswer(answer).when(metricsStore).writeMetrics(eq(2L), any());
    metricsSink.snapshotState(context);
  }

  @Test
  void metricsSinkShouldClearPersistedMetrics() {

    var key1 = new RowKey(getTestRow(), List.of("aaa", "bbb", "ccc"));
    var key2 = new RowKey(getTestRow(), List.of("aaa", "bbb"));

    var context = mock(FunctionSnapshotContext.class);
    when(context.getCheckpointId()).thenReturn(2L);

    var metricsStore = mock(MetricsStore.class);
    var metricsSink = new MetricsSink(metricsStore);
    metricsSink.invoke(new Metric(key1, 1L), null);
    metricsSink.invoke(new Metric(key2, 10000L), null);

    metricsSink.snapshotState(context);
    ArgumentCaptor<Metrics> captor = ArgumentCaptor.forClass(Metrics.class);
    verify(metricsStore, times(1)).writeMetrics(eq(2L), captor.capture());
    assertEquals(new Metrics(), captor.getAllValues().get(0));
  }
}
