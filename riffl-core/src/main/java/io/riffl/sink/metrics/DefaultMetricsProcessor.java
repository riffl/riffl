package io.riffl.sink.metrics;

import io.riffl.config.ConfigUtils;
import io.riffl.config.Sink;
import io.riffl.sink.row.RowKey;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.function.Consumer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMetricsProcessor implements MetricsProcessor {

  private static final Logger logger = LoggerFactory.getLogger(DefaultMetricsProcessor.class);
  static final String PROPERTIES_METRIC_TYPE = "metricType";

  private final MetricType metricType;

  // should be fixed in size
  private final HashMap<RowKey, Long> store = new HashMap<>();
  private final Path metricsPath;
  private final Sink sink;

  private boolean hasMetricsToConsume = false;

  public enum MetricType {
    COUNT,
    BYTES
  }

  public DefaultMetricsProcessor(Sink sink, Path metricsPath) {
    this.sink = sink;
    this.metricsPath = metricsPath;
    this.metricType =
        ConfigUtils.enumProperty(
            sink.getDistribution().getProperties().get(PROPERTIES_METRIC_TYPE),
            MetricType.class,
            MetricType.COUNT);
  }

  @Override
  public void addMetric(RowKey key, Row row) {
    var value = metricType == MetricType.COUNT ? 1L : row.toString().getBytes().length;
    store.merge(key, value, Long::sum);
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
    store.forEach((key, value) -> consumer.accept(new Metric(key, value)));
    store.clear();
    this.hasMetricsToConsume = false;
  }

  // Todo: add caching layer
  @Override
  public Metrics getMetrics(long checkpointId) {
    Metrics metrics;
    if (checkpointId > 1) {
      var path = new Path(metricsPath.toString() + (checkpointId - 1));
      var removePath = new Path(metricsPath.toString() + (checkpointId - 2));

      try {
        var fs = FileSystem.getUnguardedFileSystem(path.toUri());

        try (var file = fs.open(path)) {
          ObjectInputStream objectInput = new ObjectInputStream(file);

          metrics = (Metrics) objectInput.readObject();
          logger.info(
              "Loaded metrics for checkpoint: {}, sink {}: {}",
              checkpointId - 1,
              sink.getTable(),
              metrics);
          if (fs.exists(removePath)) {
            fs.delete(removePath, false);
            logger.info(
                "Deleted metrics for checkpoint: {}, sink {}, path {}",
                checkpointId - 2,
                sink.getTable(),
                removePath);
          }
          return metrics;
        } catch (IOException | ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      metrics = new Metrics();
    }

    return metrics;
  }
}
