package io.riffl.sink.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.riffl.config.Distribution;
import io.riffl.config.Sink;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

public class KeyByTaskAssignerTests {

  Properties properties;

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
            "",
            new Distribution(KeyByTaskAssigner.class.getCanonicalName(), properties),
            tasks.size());
    var tasksAssignment = new TasksAssignment();
    var tasksAssigner = new KeyByTaskAssigner(sink, tasks, tasksAssignment);

    Row row = Row.withNames(RowKind.INSERT);
    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);

    var key = tasksAssigner.getKey(row);

    assertNull(tasksAssignment.get(key));
    tasksAssigner.getTask(row, key);
    assertEquals(2, tasksAssignment.get(key).size());
  }
}
