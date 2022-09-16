package io.riffl.config;

import java.io.Serializable;
import java.util.Map;

public class Distribution implements Serializable {

  final String className;
  final Map<String, Object> properties;
  final Integer parallelism;

  public Distribution(String className, Map<String, Object> properties, Integer parallelism) {
    this.className = className;
    this.properties = properties;
    this.parallelism = parallelism;
  }

  public String getClassName() {
    return className;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public Integer getParallelism() {
    return parallelism;
  }

  public boolean hasParallelism() {
    return parallelism != null;
  }
}
