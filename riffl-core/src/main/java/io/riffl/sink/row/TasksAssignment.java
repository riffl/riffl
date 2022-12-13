package io.riffl.sink.row;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TasksAssignment implements Serializable {

  private final Map<RowKey, List<Integer>> store = new HashMap<>();

  void put(RowKey key, List<Integer> value) {
    store.put(key, value);
  }

  public boolean containsKey(RowKey key) {
    return store.containsKey(key);
  }

  public List<Integer> get(RowKey key) {
    return store.get(key);
  }

  public void clear() {
    store.clear();
  }
}
