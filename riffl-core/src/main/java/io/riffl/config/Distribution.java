package io.riffl.config;

import java.io.Serializable;
import java.util.Properties;

public class Distribution implements Serializable {

  final String className;
  final Properties properties;

  public Distribution(String className, Properties properties) {
    this.className = className;
    this.properties = properties;
  }

  public String getClassName() {
    return className;
  }

  public Properties getProperties() {
    return properties;
  }
}
