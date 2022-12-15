package io.riffl.sink.metrics;

import io.riffl.config.ConfigUtils;
import io.riffl.config.Sink;
import io.riffl.sink.row.RowKey;
import java.util.HashMap;
import java.util.function.Consumer;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMetricsProcessor implements MetricsProcessor {

  private static final Logger logger = LoggerFactory.getLogger(DefaultMetricsProcessor.class);
  static final String PROPERTIES_METRIC_TYPE = "metricType";
  static final int DEFAULT_METRICS_NUM_RETAINED = 5;

  private final MetricType metricType;

  // should be a fixed in size sketch
  private final HashMap<RowKey, Long> metricValueStore = new HashMap<>();

  private boolean hasMetricsToConsume = false;

  public enum MetricType {
    COUNT,
    BYTES
  }

  final MetricsStore externalMetricsStore;

  public DefaultMetricsProcessor(Sink sink, MetricsStore metricsStore) {
    this.metricType =
        ConfigUtils.enumProperty(
            sink.getDistribution().getProperties().get(PROPERTIES_METRIC_TYPE),
            MetricType.class,
            MetricType.COUNT);
    this.externalMetricsStore = metricsStore;
  }

  @Override
  public void addMetric(RowKey key, Row row) {
    var value = metricType == MetricType.COUNT ? 1L : row.toString().getBytes().length;
    metricValueStore.merge(key, value, Long::sum);
  }

  @Override
  public void flushMetrics(long checkpointId) {
    hasMetricsToConsume = true;
  }

  @Override
  public boolean hasMetricsToWrite() {
    return hasMetricsToConsume;
  }

  @Override
  public void consumeMetrics(Consumer<Metric> consumer) {
    metricValueStore.forEach((key, value) -> consumer.accept(new Metric(key, value)));
    metricValueStore.clear();
    this.hasMetricsToConsume = false;
  }

  @Override
  public Metrics getMetrics(long checkpointId) {
    if (checkpointId > 1) {
      externalMetricsStore.removeMetrics(checkpointId - DEFAULT_METRICS_NUM_RETAINED);
      return externalMetricsStore.loadMetrics(checkpointId - 1);
    } else {
      return new Metrics();
    }
  }
}
