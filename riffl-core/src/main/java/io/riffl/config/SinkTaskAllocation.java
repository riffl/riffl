package io.riffl.config;

public class SinkTaskAllocation {

  private final String className;

  public SinkTaskAllocation(String className) {
    this.className = className;
  }

  public String getClassName() {
    return className;
  }
}
