package io.riffl.sink.metrics;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilesystemMetricsStore implements MetricsStore, Serializable {

  private static final Logger logger = LoggerFactory.getLogger(FilesystemMetricsStore.class);
  final Path basePath;

  protected static final LoadingCache<PathKey, Metrics> cache =
      Caffeine.newBuilder().build(FilesystemMetricsStore::loadMetricsFromFile);
  private final Boolean skipOnFailure;

  protected static Metrics loadMetricsFromFile(PathKey pathKey) {
    try {
      var fs = FileSystem.getUnguardedFileSystem(pathKey.path.toUri());
      try (var file = fs.open(pathKey.path)) {
        ObjectInputStream objectInput = new ObjectInputStream(file);
        var metrics = (Metrics) objectInput.readObject();
        logger.info("Loaded metrics for path: {},  {}", pathKey, metrics);
        return metrics;
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      if (pathKey.skipOnFailure) {
        logger.warn("Metrics for path: {}, could not be loaded {}", pathKey, e);
        return new Metrics();
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  public FilesystemMetricsStore(Path basePath) {
    this(basePath, false);
  }

  public FilesystemMetricsStore(Path basePath, Boolean skipOnFailure) {
    this.basePath = basePath;
    this.skipOnFailure = skipOnFailure;
  }

  @Override
  public Metrics loadMetrics(long checkpointId) {
    return cache.get(new PathKey(new Path(basePath.toString() + checkpointId), skipOnFailure));
  }

  @Override
  public void removeMetrics(long checkpointId) {
    var path = new Path(basePath.toString() + checkpointId);
    try {
      var fs = FileSystem.getUnguardedFileSystem(path.toUri());
      if (fs.exists(path)) {
        fs.delete(path, false);

        cache.invalidate(new PathKey(path, skipOnFailure));
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

  private static class PathKey {

    private final Path path;
    private final Boolean skipOnFailure;

    private PathKey(org.apache.flink.core.fs.Path path, Boolean skipOnFailure) {
      this.path = path;
      this.skipOnFailure = skipOnFailure;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PathKey pathKey = (PathKey) o;
      return path.equals(pathKey.path) && skipOnFailure.equals(pathKey.skipOnFailure);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, skipOnFailure);
    }
  }
}
