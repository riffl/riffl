package io.riffl.sink.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.riffl.config.Distribution;
import io.riffl.config.Sink;
import io.riffl.sink.metrics.TaskAssignerMetrics;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DistributeByTaskAssignerTests {

  Properties properties;
  TaskAssignerMetrics metrics;
  TaskAssignment taskAssignment;

  FunctionSnapshotContext context =
      new FunctionSnapshotContext() {
        @Override
        public long getCheckpointId() {
          return 0L;
        }

        @Override
        public long getCheckpointTimestamp() {
          return 0L;
        }
      };

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put(DistributeByTaskAssigner.PROPERTIES_KEYS, List.of("aaa", "bbb", "ccc"));
    taskAssignment = new TaskAssignment();
    metrics =
        new TaskAssignerMetrics() {
          private final Map<List<Object>, Long> store = new HashMap<>();

          @Override
          public void add(List<Object> key, Long value) {
            store.put(key, value);
          }

          @Override
          public Map<List<Object>, Long> getMetrics() {
            return store;
          }

          @Override
          public void clear(long checkpointId) {}
        };
  }

  private TaskAssigner getTaskAssigner(
      List<Integer> tasks,
      Properties properties,
      TaskAssignerMetrics metrics,
      TaskAssignment assignment) {
    Sink sink =
        new Sink(
            "",
            "",
            "",
            new Distribution(
                DistributeByTaskAssigner.class.getCanonicalName(), properties, tasks.size()));

    var taskAssigner = new DistributeByTaskAssigner(assignment, metrics);
    taskAssigner.configure(sink, tasks);
    return taskAssigner;
  }

  @Test
  void tasksAssignedAccordingToWeightSkewed() throws Exception {
    List<Integer> tasks = IntStream.rangeClosed(0, 15).boxed().collect(Collectors.toList());
    var taskAssigner = getTaskAssigner(tasks, properties, metrics, taskAssignment);

    metrics.add(List.of("aaa", 1), 1000L);
    metrics.add(List.of("bbb", 1), 10L);
    metrics.add(List.of("ccc", 1), 10L);
    metrics.add(List.of("ddd", 1), 10L);
    metrics.add(List.of("eee", 1), 500L);
    metrics.add(List.of("fff", 1), (long) Integer.MAX_VALUE);

    ((CheckpointedFunction) taskAssigner).snapshotState(context);

    assertEquals(
        taskAssignment.get(List.of("fff", 1)),
        IntStream.rangeClosed(0, 14).boxed().collect(Collectors.toList()));
    assertEquals(taskAssignment.get(List.of("aaa", 1)), List.of(15));
    assertEquals(taskAssignment.get(List.of("eee", 1)), List.of(15));
    assertEquals(taskAssignment.get(List.of("bbb", 1)), List.of(15));
    assertEquals(taskAssignment.get(List.of("ccc", 1)), List.of(15));
    assertEquals(taskAssignment.get(List.of("ddd", 1)), List.of(15));
  }

  @Test
  void tasksAssignedAccordingToWeightBalanced() throws Exception {
    List<Integer> tasks = IntStream.rangeClosed(0, 15).boxed().collect(Collectors.toList());
    var taskAssigner = getTaskAssigner(tasks, properties, metrics, taskAssignment);

    metrics.add(List.of("aaa", 1), 1000L);
    metrics.add(List.of("bbb", 1), 10L);
    metrics.add(List.of("ccc", 1), 10L);
    metrics.add(List.of("ddd", 1), 10L);
    metrics.add(List.of("eee", 1), 500L);

    ((CheckpointedFunction) taskAssigner).snapshotState(context);
    assertEquals(
        taskAssignment.get(List.of("aaa", 1)),
        IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList()));
    assertEquals(
        taskAssignment.get(List.of("eee", 1)),
        IntStream.rangeClosed(10, 14).boxed().collect(Collectors.toList()));
    assertEquals(taskAssignment.get(List.of("bbb", 1)), List.of(15));
    assertEquals(taskAssignment.get(List.of("ccc", 1)), List.of(15));
    assertEquals(taskAssignment.get(List.of("ddd", 1)), List.of(15));
  }

  @Test
  void tasksAssignedForSingleEntry() throws Exception {
    List<Integer> tasks = IntStream.rangeClosed(0, 15).boxed().collect(Collectors.toList());
    var taskAssigner = getTaskAssigner(tasks, properties, metrics, taskAssignment);
    metrics.add(List.of("aaa", 1), 1000L);

    ((CheckpointedFunction) taskAssigner).snapshotState(context);
    assertEquals(taskAssignment.get(List.of("aaa", 1)), tasks);
  }

  @Test
  void tasksShouldBeAssigned_AccordingToWeightEqual() throws Exception {
    List<Integer> tasks = IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList());
    var taskAssigner = getTaskAssigner(tasks, properties, metrics, taskAssignment);

    metrics.add(List.of("aaa", 1), 501L);
    metrics.add(List.of("bbb", 1), 502L);
    metrics.add(List.of("ccc", 1), 503L);

    ((CheckpointedFunction) taskAssigner).snapshotState(context);

    assertEquals(
        taskAssignment.get(List.of("aaa", 1)),
        IntStream.rangeClosed(6, 8).boxed().collect(Collectors.toList()));
    assertEquals(
        taskAssignment.get(List.of("bbb", 1)),
        IntStream.rangeClosed(3, 5).boxed().collect(Collectors.toList()));
    assertEquals(
        taskAssignment.get(List.of("ccc", 1)),
        IntStream.rangeClosed(0, 2).boxed().collect(Collectors.toList()));
  }

  @Test
  void tasksShouldBeAssignedOnLowThroughput() throws Exception {
    List<Integer> tasks = IntStream.rangeClosed(0, 50000).boxed().collect(Collectors.toList());
    var taskAssigner = getTaskAssigner(tasks, properties, metrics, taskAssignment);

    metrics.add(List.of("aaa", 1), 25L);
    metrics.add(List.of("bbb", 1), 25L);

    ((CheckpointedFunction) taskAssigner).snapshotState(context);

    assertEquals(tasks.subList(0, 25), taskAssignment.get(List.of("aaa", 1)));
    assertEquals(tasks.subList(25, 50), taskAssignment.get(List.of("bbb", 1)));
  }

  @Test
  void rowShouldBeHandledWithStandardValues() throws Exception {
    List<Integer> tasks = IntStream.rangeClosed(0, 1).boxed().collect(Collectors.toList());
    var taskAssigner = getTaskAssigner(tasks, properties, metrics, taskAssignment);

    Row row = Row.withNames(RowKind.INSERT);

    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);

    assertTrue(tasks.contains(taskAssigner.taskIndex(row)));

    ((CheckpointedFunction) taskAssigner).snapshotState(context);

    List<Object> key = new ArrayList<>(Arrays.asList("aaaData", 0.1d, null));

    assertTrue(metrics.getMetrics().containsKey(key));
    assertEquals(1, metrics.getMetrics().get(key));
  }

  @Test
  void tasksShouldBeAssignedAccordingToWeightWithUnorderedTasks() throws Exception {
    List<Integer> tasks = List.of(1, 0, 9, 5, 3);
    var taskAssigner = getTaskAssigner(tasks, properties, metrics, taskAssignment);

    IntStream.rangeClosed(0, 2)
        .forEach(
            l -> {
              IntStream.rangeClosed(0, 9).forEach(s -> metrics.add(List.of(l, s), 140L));
            });
    metrics.add(List.of(1, 1), 7170L);

    ((CheckpointedFunction) taskAssigner).snapshotState(context);

    IntStream.rangeClosed(0, 2)
        .forEach(
            l -> {
              IntStream.rangeClosed(0, 9)
                  .forEach(
                      i -> {
                        if (List.of(1, 1).equals(List.of(l, i))) {
                          assertEquals(List.of(1, 0, 9), taskAssignment.get(List.of(l, i)));
                        } else {
                          assertTrue(
                              List.of(5, 3).contains(taskAssignment.get(List.of(l, i)).get(0)));
                        }
                      });
            });
  }
}
