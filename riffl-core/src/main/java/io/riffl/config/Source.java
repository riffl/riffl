package io.riffl.config;

public class Source {

  final String createUri;
  final String mapUri;

  final Boolean rebalance;

  public Source(String createUri, String mapUri, boolean rebalance) {
    this.createUri = createUri;
    this.mapUri = mapUri;
    this.rebalance = rebalance;
  }

  public String getCreateUri() {
    return createUri;
  }

  public String getMapUri() {
    return mapUri;
  }

  public Boolean getRebalance() {
    return rebalance;
  }
}
