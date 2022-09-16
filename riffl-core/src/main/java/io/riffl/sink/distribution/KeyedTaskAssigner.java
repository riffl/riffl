package io.riffl.sink.distribution;

import io.riffl.config.Sink;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyedTaskAssigner implements TaskAssigner, Serializable {

  private static final String PROPERTIES_KEYS = "keys";
  private static final String PROPERTIES_KEY_PARALLELISM = "keyParallelism";

  private static final Logger logger = LoggerFactory.getLogger(KeyedTaskAssigner.class);

  private List<Integer> tasks;
  private ThreadLocalRandom random;
  private List<String> keys;

  private final Map<List<Object>, List<Integer>> store = new HashMap<>();
  private Integer keyParallelism;

  @Override
  public void configure(Sink sink, List<Integer> tasks) {
    this.tasks = tasks;
    this.random = ThreadLocalRandom.current();
    Object keysObject = sink.getDistribution().getProperties().get(PROPERTIES_KEYS);
    try {
      this.keys = ((List<?>) keysObject).stream().map(e -> (String) e).collect(Collectors.toList());
    } catch (ClassCastException e) {
      throw new RuntimeException(
          MessageFormat.format("{} must be a list of {}", keysObject, String.class));
    }

    keyParallelism =
        (Integer) sink.getDistribution().getProperties().get(PROPERTIES_KEY_PARALLELISM);

    logger.info("{} created with tasks {}", sink, tasks);
  }

  @Override
  public int taskIndex(Row row) {
    List<Object> key = keys.stream().map(row::getField).collect(Collectors.toList());
    Integer tasksIndex;
    if (!store.containsKey(key)) {
      tasks = tasks.stream().sorted().collect(Collectors.toList());

      Collections.shuffle(tasks, new Random(key.hashCode()));

      List<Integer> keyTasks = tasks.subList(0, keyParallelism);
      store.put(key, keyTasks);
      logger.info(
          "Key {}, hashCode {}, created with keyTasks {}, out of {}",
          key,
          key.hashCode(),
          keyTasks,
          tasks);
    }
    tasksIndex = store.get(key).get(random.nextInt(store.get(key).size()));
    return tasksIndex;
  }
}
