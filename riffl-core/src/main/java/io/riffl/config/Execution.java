package io.riffl.config;

import java.util.Properties;

public class Execution {

  enum Type {
    FLINK
  }

  private final Type type;
  private final Properties properties;

  public Execution(Type type, Properties properties) {
    this.type = type;
    this.properties = properties;
  }

  public Type getType() {
    return type;
  }

  public Properties getProperties() {
    return properties;
  }
}
