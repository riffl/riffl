package io.riffl.sink.row;

import io.riffl.config.ConfigUtils;
import io.riffl.config.Sink;
import io.riffl.sink.metrics.Metrics;
import io.riffl.sink.row.tasks.TaskAssigner;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributeByTaskAssigner implements TaskAssigner, Serializable {

  static final String PROPERTIES_KEYS = "keys";

  private final List<String> keyColumns;

  private final List<Integer> tasks;
  private final ThreadLocalRandom random;

  private final TasksAssignment tasksAssignment;

  private static final Logger logger = LoggerFactory.getLogger(DistributeByTaskAssigner.class);

  public DistributeByTaskAssigner(
      Sink sink, List<Integer> tasks, TasksAssignment tasksAssignment, Metrics metrics) {

    this.random = ThreadLocalRandom.current();
    this.tasks = tasks;
    this.keyColumns =
        ConfigUtils.listProperty(sink.getDistribution().getProperties().get(PROPERTIES_KEYS));
    this.tasksAssignment = tasksAssignment;
    calculateAssignment(tasks, this.tasksAssignment, metrics);

    if (logger.isDebugEnabled()) {
      var assignment =
          metrics.entrySet().stream()
              .map(Entry::getKey)
              .map(this.tasksAssignment::get)
              .collect(Collectors.toList());

      logger.debug(
          "{}: metrics {}, tasks {}, assignment {}", sink.getTable(), metrics, tasks, assignment);
    }
  }

  @Override
  public RowKey getKey(Row row) {
    return new RowKey(row, keyColumns);
  }

  private void calculateAssignment(
      List<Integer> tasks, TasksAssignment keyTasksAssignment, Metrics metrics) {
    keyTasksAssignment.clear();
    var keysMetrics = metrics.entrySet();
    if (keysMetrics != null && !keysMetrics.isEmpty()) {
      if (keysMetrics.size() == 1) {
        keyTasksAssignment.put(keysMetrics.stream().map(Entry::getKey).iterator().next(), tasks);
      } else {
        var stateSorted =
            keysMetrics.stream()
                .sorted(Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());

        long rowsRemaining = keysMetrics.stream().map(Entry::getValue).reduce(Long::sum).orElse(0L);
        long rowsTarget = (long) Math.ceil((double) rowsRemaining / tasks.size());
        int currentTask = 0;

        for (Entry<RowKey, Long> item : stateSorted) {
          if (rowsTarget != 0 && item.getValue() >= rowsTarget) {
            var previousTask = currentTask;
            currentTask += item.getValue() / rowsTarget;
            rowsRemaining -= item.getValue();
            rowsTarget = (long) Math.ceil((double) rowsRemaining / (tasks.size() - currentTask));
            keyTasksAssignment.put(item.getKey(), tasks.subList(previousTask, currentTask));
          } else {
            // distribute light keys over remaining tasks
            var position = Math.floorMod(item.getKey().hashCode(), (tasks.size() - currentTask));
            keyTasksAssignment.put(
                item.getKey(), Collections.singletonList(tasks.get((tasks.size() - 1) - position)));
          }
        }
      }
    }
  }

  @Override
  public int getTask(Row row, RowKey key) {
    Integer tasksIndex;
    if (tasksAssignment.containsKey(key)) {
      tasksIndex = tasksAssignment.get(key).get(random.nextInt(tasksAssignment.get(key).size()));
    } else {
      tasksIndex = tasks.get(random.nextInt(tasks.size()));
    }
    return tasksIndex;
  }
}
