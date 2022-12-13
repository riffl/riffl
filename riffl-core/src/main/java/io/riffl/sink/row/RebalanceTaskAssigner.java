package io.riffl.sink.row;

import io.riffl.config.Sink;
import io.riffl.sink.row.tasks.TaskAssigner;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceTaskAssigner implements TaskAssigner, Serializable {
  private static final Logger logger = LoggerFactory.getLogger(RebalanceTaskAssigner.class);

  private final List<Integer> tasks;
  private final ThreadLocalRandom random;

  public RebalanceTaskAssigner(Sink sink, List<Integer> tasks, TasksAssignment tasksAssignment) {
    this.random = ThreadLocalRandom.current();
    this.tasks = tasks;
    logger.debug(MessageFormat.format("{0} created with tasks {1}", sink, tasks));
  }

  @Override
  public RowKey getKey(Row row) {
    return null;
  }

  @Override
  public int getTask(Row keys, RowKey key) {
    return tasks.get(random.nextInt(tasks.size()));
  }
}
