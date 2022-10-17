package io.riffl.config;

import java.io.Serializable;
import java.util.Objects;

public class Sink implements Serializable {
  final String create;
  final String query;
  final Distribution distribution;

  public Sink(String create, String query, Distribution distribution) {
    this.create = create;
    this.query = query;
    this.distribution = distribution;
  }

  public String getCreate() {
    return create;
  }

  public String getQuery() {
    return query;
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
    return create.equals(sink.create)
        && Objects.equals(query, sink.query)
        && Objects.equals(distribution, sink.distribution);
  }

  @Override
  public int hashCode() {
    return Objects.hash(create, query, distribution);
  }
}
