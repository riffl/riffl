package io.riffl.config;

import java.io.Serializable;
import java.util.Objects;

public class Sink implements Serializable {
  final String createUri;
  final String queryUri;
  final Distribution distribution;

  public Sink(String createUri, String queryUri, Distribution distribution) {
    this.createUri = createUri;
    this.queryUri = queryUri;
    this.distribution = distribution;
  }

  public String getCreateUri() {
    return createUri;
  }

  public String getQueryUri() {
    return queryUri;
  }

  public Distribution getDistribution() {
    return distribution;
  }

  public boolean hasDistribution() {
    return distribution != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Sink sink = (Sink) o;
    return createUri.equals(sink.createUri)
        && Objects.equals(queryUri, sink.queryUri)
        && Objects.equals(distribution, sink.distribution);
  }

  @Override
  public int hashCode() {
    return Objects.hash(createUri, queryUri, distribution);
  }
}
