package io.riffl.utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

public class FilesystemUtils {
  public static String openFileAsString(Path path) {
    try {
      FileSystem fs = FileSystem.getUnguardedFileSystem(path.toUri());
      try (FSDataInputStream is = fs.open(path)) {
        return new String(is.readAllBytes(), StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
