package io.riffl.sink.distribution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.riffl.config.Distribution;
import io.riffl.config.Sink;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

public class KeyByTaskAssignerTests {

  Properties properties;
  TaskAssignment taskAssignment = new TaskAssignment();
  TaskAssigner taskAssigner = new KeyByTaskAssigner(taskAssignment);

  @Test
  void tasksAssignedAccordingToKeyConfiguration() {
    properties = new Properties();
    properties.put(KeyByTaskAssigner.PROPERTIES_KEYS, List.of("aaa", "bbb", "ccc"));
    properties.put(KeyByTaskAssigner.PROPERTIES_KEY_PARALLELISM, 2);
    List<Integer> tasks = IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList());
    Sink sink =
        new Sink(
            "",
            "",
            new Distribution(KeyByTaskAssigner.class.getCanonicalName(), properties, tasks.size()));

    taskAssigner.configure(sink, tasks);

    Row row = Row.withNames(RowKind.INSERT);
    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);

    List<Object> key = new ArrayList<>(Arrays.asList("aaaData", 0.1d, null));

    assertNull(taskAssignment.get(key));

    taskAssigner.taskIndex(row);
    assertEquals(2, taskAssignment.get(key).size());
  }
}
