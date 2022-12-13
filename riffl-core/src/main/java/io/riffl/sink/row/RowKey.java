package io.riffl.sink.row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.types.Row;

@TypeInfo(RowKeyTypeInfo.class)
public class RowKey implements Serializable {

  private static final long serialVersionUID = -1954637662754631415L;
  private List<Integer> key;

  public RowKey() {}

  public RowKey(Row row, List<String> columns) {
    this.key = new ArrayList<>();
    for (var k : columns) {
      if (row.getField(k) != null) {
        key.add(Objects.requireNonNull(row.getField(k)).hashCode());
      } else {
        key.add(null);
      }
    }
  }

  public List<Integer> getKey() {
    return key;
  }

  public void setKey(List<Integer> key) {
    this.key = key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowKey rowKey = (RowKey) o;
    return key.equals(rowKey.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }

  @Override
  public String toString() {
    return key.toString();
  }
}
