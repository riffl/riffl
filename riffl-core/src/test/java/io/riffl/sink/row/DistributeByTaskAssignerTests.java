package io.riffl.sink.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.riffl.config.Distribution;
import io.riffl.config.Sink;
import io.riffl.sink.metrics.Metrics;
import io.riffl.sink.row.tasks.TaskAssigner;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DistributeByTaskAssignerTests {

  Properties properties;
  Metrics metrics;
  TasksAssignment taskAssignment;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put(DistributeByTaskAssigner.PROPERTIES_KEYS, List.of("aaa", "bbb", "ccc"));
    taskAssignment = new TasksAssignment();
    metrics = new Metrics();
  }

  private TaskAssigner createTaskAssigner(
      List<Integer> tasks, Properties properties, Metrics metrics, TasksAssignment assignment) {
    Sink sink =
        new Sink(
            "",
            "",
            "",
            new Distribution(DistributeByTaskAssigner.class.getCanonicalName(), properties),
            tasks.size());

    return new DistributeByTaskAssigner(sink, tasks, assignment, metrics);
  }

  private RowKey getKey(List<Object> keyValues) {
    Row row = Row.withNames(RowKind.INSERT);
    ArrayList<String> key = new ArrayList<>();

    for (int i = 0; i < keyValues.size(); i++) {
      row.setField(Integer.toString(i), keyValues.get(i));
      key.add(Integer.toString(i));
    }

    return new RowKey(row, key);
  }

  @Test
  void tasksAssignedAccordingToWeightSkewed() {
    List<Integer> tasks = IntStream.rangeClosed(0, 15).boxed().collect(Collectors.toList());

    metrics.add(getKey(List.of("aaa", 1)), 1000L);
    metrics.add(getKey(List.of("bbb", 1)), 10L);
    metrics.add(getKey(List.of("ccc", 1)), 10L);
    metrics.add(getKey(List.of("ddd", 1)), 10L);
    metrics.add(getKey(List.of("eee", 1)), 500L);
    metrics.add(getKey(List.of("fff", 1)), (long) Integer.MAX_VALUE);

    createTaskAssigner(tasks, properties, metrics, taskAssignment);

    assertEquals(
        taskAssignment.get(getKey(List.of("fff", 1))),
        IntStream.rangeClosed(0, 14).boxed().collect(Collectors.toList()));
    assertEquals(taskAssignment.get(getKey(List.of("aaa", 1))), List.of(15));
    assertEquals(taskAssignment.get(getKey(List.of("eee", 1))), List.of(15));
    assertEquals(taskAssignment.get(getKey(List.of("bbb", 1))), List.of(15));
    assertEquals(taskAssignment.get(getKey(List.of("ccc", 1))), List.of(15));
    assertEquals(taskAssignment.get(getKey(List.of("ddd", 1))), List.of(15));
  }

  @Test
  void tasksAssignedAccordingToWeightBalanced() {
    List<Integer> tasks = IntStream.rangeClosed(0, 15).boxed().collect(Collectors.toList());
    metrics.add(getKey(List.of("aaa", 1)), 1000L);
    metrics.add(getKey(List.of("bbb", 1)), 10L);
    metrics.add(getKey(List.of("ccc", 1)), 10L);
    metrics.add(getKey(List.of("ddd", 1)), 10L);
    metrics.add(getKey(List.of("eee", 1)), 500L);

    createTaskAssigner(tasks, properties, metrics, taskAssignment);
    assertEquals(
        taskAssignment.get(getKey(List.of("aaa", 1))),
        IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList()));
    assertEquals(
        taskAssignment.get(getKey(List.of("eee", 1))),
        IntStream.rangeClosed(10, 14).boxed().collect(Collectors.toList()));
    assertEquals(taskAssignment.get(getKey(List.of("bbb", 1))), List.of(15));
    assertEquals(taskAssignment.get(getKey(List.of("ccc", 1))), List.of(15));
    assertEquals(taskAssignment.get(getKey(List.of("ddd", 1))), List.of(15));
  }

  @Test
  void tasksAssignedWithNegativeHashCode() {
    List<Integer> tasks = IntStream.rangeClosed(0, 4).boxed().collect(Collectors.toList());
    IntStream.rangeClosed(-98, -93).forEach(r -> metrics.add(getKey(List.of(r)), (long) 1));

    createTaskAssigner(tasks, properties, metrics, taskAssignment);
    assertEquals(taskAssignment.get(getKey(List.of(-98))), List.of(0));
    assertEquals(taskAssignment.get(getKey(List.of(-97))), List.of(4));
    assertEquals(taskAssignment.get(getKey(List.of(-96))), List.of(3));
    assertEquals(taskAssignment.get(getKey(List.of(-95))), List.of(2));
    assertEquals(taskAssignment.get(getKey(List.of(-94))), List.of(1));
    assertEquals(taskAssignment.get(getKey(List.of(-93))), List.of(0));
  }

  @Test
  void tasksAssignedForSingleEntry() {
    List<Integer> tasks = IntStream.rangeClosed(0, 15).boxed().collect(Collectors.toList());

    metrics.add(getKey(List.of("aaa", 1)), 1000L);

    createTaskAssigner(tasks, properties, metrics, taskAssignment);
    assertEquals(taskAssignment.get(getKey(List.of("aaa", 1))), tasks);
  }

  @Test
  void tasksShouldBeAssigned_AccordingToWeightEqual() {
    List<Integer> tasks = IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList());

    metrics.add(getKey(List.of("aaa", 1)), 501L);
    metrics.add(getKey(List.of("bbb", 1)), 502L);
    metrics.add(getKey(List.of("ccc", 1)), 503L);

    createTaskAssigner(tasks, properties, metrics, taskAssignment);

    assertEquals(
        taskAssignment.get(getKey(List.of("aaa", 1))),
        IntStream.rangeClosed(6, 8).boxed().collect(Collectors.toList()));
    assertEquals(
        taskAssignment.get(getKey(List.of("bbb", 1))),
        IntStream.rangeClosed(3, 5).boxed().collect(Collectors.toList()));
    assertEquals(
        taskAssignment.get(getKey(List.of("ccc", 1))),
        IntStream.rangeClosed(0, 2).boxed().collect(Collectors.toList()));
  }

  @Test
  void tasksShouldBeAssignedOnLowThroughput() {
    List<Integer> tasks = IntStream.rangeClosed(0, 50000).boxed().collect(Collectors.toList());

    metrics.add(getKey(List.of("aaa", 1)), 25L);
    metrics.add(getKey(List.of("bbb", 1)), 25L);

    createTaskAssigner(tasks, properties, metrics, taskAssignment);

    assertEquals(tasks.subList(0, 25), taskAssignment.get(getKey(List.of("bbb", 1))));
    assertEquals(tasks.subList(25, 50), taskAssignment.get(getKey(List.of("aaa", 1))));
  }

  @Test
  void rowShouldBeHandledWithStandardValues() {
    List<Integer> tasks = IntStream.rangeClosed(0, 1).boxed().collect(Collectors.toList());
    var taskAssigner = createTaskAssigner(tasks, properties, metrics, taskAssignment);

    Row row = Row.withNames(RowKind.INSERT);

    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);

    assertTrue(tasks.contains(taskAssigner.getTask(row, taskAssigner.getKey(row))));
  }

  @Test
  void tasksShouldBeAssignedAccordingToWeightWithUnorderedTasks() {
    List<Integer> tasks = List.of(1, 0, 9, 5, 3);

    IntStream.rangeClosed(0, 2)
        .forEach(
            l ->
                IntStream.rangeClosed(0, 9).forEach(s -> metrics.add(getKey(List.of(l, s)), 140L)));
    metrics.add(getKey(List.of(1, 1)), 7170L);

    createTaskAssigner(tasks, properties, metrics, taskAssignment);

    IntStream.rangeClosed(0, 2)
        .forEach(
            l ->
                IntStream.rangeClosed(0, 9)
                    .forEach(
                        i -> {
                          if (List.of(1, 1).equals(List.of(l, i))) {
                            assertEquals(
                                List.of(1, 0, 9), taskAssignment.get(getKey(List.of(l, i))));
                          } else {
                            assertTrue(
                                List.of(5, 3)
                                    .contains(taskAssignment.get(getKey(List.of(l, i))).get(0)));
                          }
                        }));
  }
}
