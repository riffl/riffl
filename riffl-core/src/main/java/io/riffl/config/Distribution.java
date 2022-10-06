package io.riffl.config;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

public class Distribution implements Serializable {

  final String className;
  final Properties properties;
  final Integer parallelism;

  public Distribution(String className, Properties properties, Integer parallelism) {
    this.className = className;
    this.properties = properties;
    this.parallelism = parallelism;
  }

  public String getClassName() {
    return className;
  }

  public Properties getProperties() {
    return properties;
  }

  public Integer getParallelism() {
    return parallelism;
  }

  public boolean hasParallelism() {
    return parallelism != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Distribution that = (Distribution) o;
    return className.equals(that.className)
        && properties.equals(that.properties)
        && parallelism.equals(that.parallelism);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, properties, parallelism);
  }
}
