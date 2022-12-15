package io.riffl.sink.metrics;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilesystemMetricsStore implements MetricsStore, Serializable {

  private static final Logger logger = LoggerFactory.getLogger(FilesystemMetricsStore.class);
  final Path basePath;

  protected static final LoadingCache<Path, Metrics> cache =
      Caffeine.newBuilder().build(FilesystemMetricsStore::loadMetricsFromFile);

  protected static Metrics loadMetricsFromFile(Path path) {
    try {
      var fs = FileSystem.getUnguardedFileSystem(path.toUri());
      try (var file = fs.open(path)) {
        ObjectInputStream objectInput = new ObjectInputStream(file);
        var metrics = (Metrics) objectInput.readObject();
        logger.info("Loaded metrics for path: {},  {}", path, metrics);
        return metrics;
      } catch (IOException | ClassNotFoundException e) {
        throw new IOException(e);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public FilesystemMetricsStore(Path basePath) {
    this.basePath = basePath;
  }

  @Override
  public Metrics loadMetrics(long checkpointId) {
    return cache.get(new Path(basePath.toString() + checkpointId));
  }

  @Override
  public void removeMetrics(long checkpointId) {
    var path = new Path(basePath.toString() + checkpointId);
    try {
      var fs = FileSystem.getUnguardedFileSystem(path.toUri());
      if (fs.exists(path)) {
        fs.delete(path, false);

        cache.invalidate(path);
        logger.info("Deleted metrics for checkpoint: {}, path {}", checkpointId, path);
      }
    } catch (IOException e) {
      logger.error("Failed to delete metrics for checkpoint: {}, path {}", checkpointId, path);
    }
  }

  @Override
  public void writeMetrics(long checkpointId, Metrics metrics) {
    var path = new Path(basePath.toString() + checkpointId);
    try {
      var fs = FileSystem.getUnguardedFileSystem(path.toUri());
      try (var file = fs.create(path, WriteMode.OVERWRITE)) {
        file.write(metrics.toByteArray());
      }
      logger.info(
          "Persisted metrics for checkpoint: {}, path {}: {}",
          checkpointId,
          path,
          metrics.entrySet());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
