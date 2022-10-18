package io.riffl.config;

import java.io.Serializable;
import java.util.Objects;

public class Sink implements Serializable {
  final String create;
  final String tableIdentifier;
  final String query;
  final Distribution distribution;

  public Sink(String create, String tableIdentifier, String query, Distribution distribution) {
    this.create = create;
    this.tableIdentifier = tableIdentifier;
    this.query = query;
    this.distribution = distribution;
  }

  public String getCreate() {
    return create;
  }

  public String getTableIdentifier() {
    return tableIdentifier;
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

  public boolean hasCreate() {
    return create != null;
  }

  public boolean hasInsertIntoTable() {
    return tableIdentifier != null;
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
    return Objects.equals(create, sink.create)
        && Objects.equals(tableIdentifier, sink.tableIdentifier)
        && Objects.equals(query, sink.query)
        && Objects.equals(distribution, sink.distribution);
  }

  @Override
  public int hashCode() {
    return Objects.hash(create, tableIdentifier, query, distribution);
  }
}
