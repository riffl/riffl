package io.riffl.config;

import java.io.Serializable;

public class Sink implements Serializable {

  private final String create;
  private final String table;
  private final String query;
  private final Distribution distribution;
  private final Integer parallelism;

  public Sink(
      String create, String table, String query, Distribution distribution, Integer parallelism) {
    this.create = create;
    this.table = table;
    this.query = query;
    this.distribution = distribution;
    this.parallelism = parallelism;
  }

  public boolean hasCreate() {
    return create != null;
  }

  public String getCreate() {
    return create;
  }

  public String getTable() {
    return table;
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

  public Integer getParallelism() {
    return parallelism;
  }

  public boolean hasParallelism() {
    return parallelism != null;
  }
}
