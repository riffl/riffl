package io.riffl.sink.distribution;

import io.riffl.config.ConfigUtils;
import io.riffl.config.Sink;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyByTaskAssigner implements TaskAssigner, Serializable {

  static final String PROPERTIES_KEYS = "keys";
  static final String PROPERTIES_KEY_PARALLELISM = "keyParallelism";

  private static final Logger logger = LoggerFactory.getLogger(KeyByTaskAssigner.class);

  private List<Integer> tasks;
  private ThreadLocalRandom random;
  private List<String> keys;

  private Integer keyParallelism;

  private final TaskAssignment taskAssignment;

  public KeyByTaskAssigner() {
    this(new TaskAssignment());
  }

  public KeyByTaskAssigner(TaskAssignment taskAssignment) {
    this.taskAssignment = taskAssignment;
  }

  @Override
  public void configure(Sink sink, List<Integer> tasks) {
    this.tasks = tasks;
    this.random = ThreadLocalRandom.current();
    this.keys =
        ConfigUtils.listProperty(sink.getDistribution().getProperties().get(PROPERTIES_KEYS));
    this.keyParallelism =
        ConfigUtils.integerProperty(
            PROPERTIES_KEY_PARALLELISM, sink.getDistribution().getProperties());

    logger.debug("{} created with tasks {}", sink, tasks);
  }

  @Override
  public int taskIndex(Row row) {
    List<Object> key = new ArrayList<>();
    for (var k : keys) {
      key.add(row.getField(k));
    }
    Integer tasksIndex;
    if (!taskAssignment.containsKey(key)) {
      tasks = tasks.stream().sorted().collect(Collectors.toList());

      Collections.shuffle(tasks, new Random(key.hashCode()));

      List<Integer> keyTasks = tasks.subList(0, keyParallelism);
      taskAssignment.put(key, keyTasks);
      logger.debug(
          "Key {}, hashCode {}, created with keyTasks {}, out of {}",
          key,
          key.hashCode(),
          keyTasks,
          tasks);
    }
    tasksIndex = taskAssignment.get(key).get(random.nextInt(taskAssignment.get(key).size()));
    return tasksIndex;
  }
}
