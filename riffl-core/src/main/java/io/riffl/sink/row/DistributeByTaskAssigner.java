package io.riffl.sink.row;

import io.riffl.config.ConfigUtils;
import io.riffl.config.Sink;
import io.riffl.sink.metrics.StaticHashMapTaskAssignerMetrics;
import io.riffl.sink.metrics.TaskAssignerMetrics;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// [todo - metrics should be persisted/recovered using state]
// [todo - hold and merge previous metrics]
// [todo - store metrics externally to build task maps across entire application]
public class DistributeByTaskAssigner implements TaskAssigner, CheckpointedFunction, Serializable {

  static final String PROPERTIES_TYPE = "type";

  static final String PROPERTIES_KEYS = "keys";

  static final String PROPERTIES_METRIC_TYPE = "metricType";

  private static final Logger logger = LoggerFactory.getLogger(DistributeByTaskAssigner.class);

  private List<Integer> tasks;
  private ThreadLocalRandom random;
  private List<String> keys;

  private final TaskAssignment taskAssignment;

  private final TaskAssignerMetrics metrics;

  enum Type {
    WEIGHT
  }

  enum MetricType {
    COUNT,
    BYTES
  }

  private MetricType metricType;

  public DistributeByTaskAssigner() {
    this(new TaskAssignment(), new StaticHashMapTaskAssignerMetrics());
  }

  public DistributeByTaskAssigner(TaskAssignment taskAssignment, TaskAssignerMetrics metrics) {
    this.taskAssignment = taskAssignment;
    this.metrics = metrics;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    metrics.clear(context.getCheckpointId());
    calculateAssignment(tasks, metrics.getMetrics(), taskAssignment);

    logger.debug("Task assignment metrics {}, tasks {}", metrics.getMetrics(), tasks);

    if (logger.isDebugEnabled()) {
      metrics
          .getMetrics()
          .keySet()
          .forEach(
              key -> {
                logger.debug("Task assignment {} -> {}", key, taskAssignment.get(key));
              });
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {}

  @Override
  public void configure(Sink sink, List<Integer> tasks) {
    this.tasks = tasks;
    this.random = ThreadLocalRandom.current();
    this.keys =
        ConfigUtils.listProperty(sink.getDistribution().getProperties().get(PROPERTIES_KEYS));
    this.metricType =
        ConfigUtils.enumProperty(
            sink.getDistribution().getProperties().get(PROPERTIES_METRIC_TYPE),
            MetricType.class,
            MetricType.COUNT);

    logger.debug("{} created with tasks {}", sink, tasks);
  }

  private void calculateAssignment(
      List<Integer> tasks, Map<List<Object>, Long> keysMetrics, TaskAssignment keyTaskAssignment) {
    if (keysMetrics != null && !keysMetrics.isEmpty()) {
      if (keysMetrics.size() == 1) {
        keyTaskAssignment.put(keysMetrics.keySet().iterator().next(), tasks);
      } else {
        var stateSorted =
            keysMetrics.entrySet().stream()
                .sorted(Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());

        long rowsRemaining = keysMetrics.values().stream().reduce(Long::sum).orElse(0L);
        long rowsTarget = (long) Math.ceil((double) rowsRemaining / tasks.size());
        int currentTask = 0;

        for (Entry<List<Object>, Long> item : stateSorted) {
          if (rowsTarget != 0 && item.getValue() >= rowsTarget) {
            var previousTask = currentTask;
            currentTask += item.getValue() / rowsTarget;
            rowsRemaining -= item.getValue();
            rowsTarget = (long) Math.ceil((double) rowsRemaining / (tasks.size() - currentTask));
            keyTaskAssignment.put(item.getKey(), tasks.subList(previousTask, currentTask));
          } else {
            // distribute light keys over remaining tasks
            var position = item.getKey().hashCode() % (tasks.size() - currentTask);
            keyTaskAssignment.put(
                item.getKey(), Collections.singletonList(tasks.get((tasks.size() - 1) - position)));
          }
        }
      }
    }
  }

  @Override
  public int taskIndex(Row row) {
    List<Object> key = new ArrayList<>();
    for (var k : keys) {
      key.add(row.getField(k));
    }
    metrics.add(key, metricType == MetricType.COUNT ? 1L : row.toString().getBytes().length);

    Integer tasksIndex;
    if (taskAssignment.containsKey(key)) {
      tasksIndex = taskAssignment.get(key).get(random.nextInt(taskAssignment.get(key).size()));
    } else {
      tasksIndex = tasks.get(random.nextInt(tasks.size()));
    }
    return tasksIndex;
  }
}
