package io.riffl.sink.row;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskAssignment implements Serializable {

  private final Map<List<Object>, List<Integer>> taskAssignment = new HashMap<>();

  void put(List<Object> key, List<Integer> value) {
    taskAssignment.put(key, value);
  }

  public boolean containsKey(List<Object> key) {
    return taskAssignment.containsKey(key);
  }

  public List<Integer> get(List<Object> key) {
    return taskAssignment.get(key);
  }
}
