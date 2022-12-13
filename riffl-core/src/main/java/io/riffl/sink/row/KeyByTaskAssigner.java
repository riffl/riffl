package io.riffl.sink.row;

import io.riffl.config.ConfigUtils;
import io.riffl.config.Sink;
import io.riffl.sink.row.tasks.TaskAssigner;
import java.io.Serializable;
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

  private static final Logger logger = LoggerFactory.getLogger(KeyByFactory.class);

  private List<Integer> tasks;
  private final ThreadLocalRandom random;
  private final List<String> keys;

  private final Integer keyParallelism;

  private final TasksAssignment tasksAssignment;

  public KeyByTaskAssigner(Sink sink, List<Integer> tasks, TasksAssignment tasksAssignment) {
    this.tasks = tasks;
    this.random = ThreadLocalRandom.current();
    this.keys =
        ConfigUtils.listProperty(sink.getDistribution().getProperties().get(PROPERTIES_KEYS));
    this.keyParallelism =
        ConfigUtils.integerProperty(
            PROPERTIES_KEY_PARALLELISM, sink.getDistribution().getProperties());

    logger.debug("{} created with tasks {}", sink, tasks);
    this.tasksAssignment = tasksAssignment;
  }

  @Override
  public RowKey getKey(Row row) {
    return new RowKey(row, keys);
  }

  @Override
  public int getTask(Row row, RowKey key) {
    Integer tasksIndex;
    if (!tasksAssignment.containsKey(key)) {
      tasks = tasks.stream().sorted().collect(Collectors.toList());

      Collections.shuffle(tasks, new Random(key.hashCode()));

      List<Integer> keyTasks = tasks.subList(0, keyParallelism);
      tasksAssignment.put(key, keyTasks);
      logger.debug(
          "Key {}, hashCode {}, created with keyTasks {}, out of {}",
          key,
          key.hashCode(),
          keyTasks,
          tasks);
    }
    tasksIndex = tasksAssignment.get(key).get(random.nextInt(tasksAssignment.get(key).size()));
    return tasksIndex;
  }
}
