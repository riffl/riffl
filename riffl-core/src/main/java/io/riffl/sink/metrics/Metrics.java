package io.riffl.sink.metrics;

import io.riffl.sink.row.RowKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Metrics implements Serializable {
  private static final long serialVersionUID = -9050039770314689574L;

  private final Map<RowKey, Long> store = new HashMap<>();

  public void add(RowKey key, Long value) {
    store.merge(key, value, Long::sum);
  }

  public void clear() {
    store.clear();
  }

  public byte[] toByteArray() {
    try (var byteOut = new ByteArrayOutputStream();
        var out = new ObjectOutputStream(byteOut)) {
      out.writeObject(this);
      return byteOut.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Set<Entry<RowKey, Long>> entrySet() {
    return store.entrySet();
  }

  @Override
  public String toString() {
    return "Metrics{" + store + "}";
  }
}
