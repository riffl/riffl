package io.riffl.sink.metrics;

import io.riffl.sink.row.RowKey;

public class Metric {

  private RowKey key;
  private Long value;

  public Metric() {}

  public Metric(RowKey key, Long value) {
    this.key = key;
    this.value = value;
  }

  public RowKey getKey() {
    return key;
  }

  public void setKey(RowKey key) {
    this.key = key;
  }

  public Long getValue() {
    return value;
  }

  public void setValue(Long value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "Metric{" + "key=" + key + ", value=" + value + '}';
  }
}
