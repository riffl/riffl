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
import java.util.OptionalLong;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
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

  private FunctionInitializationContext getInitializationContext(
      Long checkpointId, boolean isRestored) throws Exception {
    var context = mock(FunctionInitializationContext.class);
    var operatorStateStore = mock(OperatorStateStore.class);
    @SuppressWarnings("unchecked")
    var listState = (ListState<Object>) mock(ListState.class);
    when(context.getRestoredCheckpointId()).thenReturn(OptionalLong.of(checkpointId));
    when(context.isRestored()).thenReturn(isRestored);
    when(context.getOperatorStateStore()).thenReturn(operatorStateStore);
    when(operatorStateStore.getListState(any())).thenReturn(listState);
    return context;
  }

  @Test
  void metricsSinkShouldPersistMetricsWithMetricsStore() throws Exception {

    var key1 = new RowKey(getTestRow(), List.of("aaa", "bbb", "ccc"));
    var key2 = new RowKey(getTestRow(), List.of("aaa", "bbb"));

    var context = mock(FunctionSnapshotContext.class);
    when(context.getCheckpointId()).thenReturn(2L);

    var metricsStore = mock(MetricsStore.class);
    var metricsSink = new MetricsSink(metricsStore);
    metricsSink.initializeState(getInitializationContext(2L, false));
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
  void metricsSinkShouldClearPersistedMetrics() throws Exception {

    var key1 = new RowKey(getTestRow(), List.of("aaa", "bbb", "ccc"));
    var key2 = new RowKey(getTestRow(), List.of("aaa", "bbb"));

    var context = mock(FunctionSnapshotContext.class);
    when(context.getCheckpointId()).thenReturn(2L);

    var metricsStore = mock(MetricsStore.class);
    var metricsSink = new MetricsSink(metricsStore);
    metricsSink.initializeState(getInitializationContext(2L, false));
    metricsSink.invoke(new Metric(key1, 1L), null);
    metricsSink.invoke(new Metric(key2, 10000L), null);

    metricsSink.snapshotState(context);
    ArgumentCaptor<Metrics> captor = ArgumentCaptor.forClass(Metrics.class);
    verify(metricsStore, times(1)).writeMetrics(eq(2L), captor.capture());
    assertEquals(new Metrics(), captor.getAllValues().get(0));
  }

  @Test
  void metricsSinkShouldBeInitialized() throws Exception {

    var key1 = new RowKey(getTestRow(), List.of("aaa", "bbb", "ccc"));
    var key2 = new RowKey(getTestRow(), List.of("aaa", "bbb"));

    var context = mock(FunctionSnapshotContext.class);
    when(context.getCheckpointId()).thenReturn(2L);

    var metricsStore = mock(MetricsStore.class);
    var metrics = new Metrics();
    metrics.add(key1, 1L);
    metrics.add(key2, 10000L);
    when(metricsStore.loadMetrics(2L)).thenReturn(metrics);

    var metricsSink = new MetricsSink(metricsStore);
    metricsSink.initializeState(getInitializationContext(3L, true));

    Answer<Metrics> answer =
        invocation -> {
          assertEquals(metrics, invocation.getArgument(1, Metrics.class));
          return invocation.getArgument(1);
        };

    doAnswer(answer).when(metricsStore).writeMetrics(eq(2L), any());

    metricsSink.snapshotState(context);
  }
}
