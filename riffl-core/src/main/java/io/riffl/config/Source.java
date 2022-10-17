package io.riffl.config;

public class Source {

  final String create;
  final String map;

  final Boolean rebalance;

  public Source(String create, String map, boolean rebalance) {
    this.create = create;
    this.map = map;
    this.rebalance = rebalance;
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
}
