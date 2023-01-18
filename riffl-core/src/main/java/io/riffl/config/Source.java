package io.riffl.config;

public class Source {

  private final String create;
  private final String map;
  private final Boolean rebalance;
  private final Integer parallelism;

  public Source(String create, String map, boolean rebalance, Integer parallelism) {
    this.create = create;
    this.map = map;
    this.rebalance = rebalance;
    this.parallelism = parallelism;
  }

  public String getCreate() {
    return create;
  }

  public String getMap() {
    return map;
  }

  public Boolean getRebalance() {
    return rebalance;
  }

  public Integer getParallelism() {
    return parallelism;
  }
}
