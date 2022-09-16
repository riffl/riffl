package io.riffl.sink.distribution;

import io.riffl.config.Sink;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceTaskAssigner implements TaskAssigner, Serializable {
  private static final Logger logger = LoggerFactory.getLogger(RebalanceTaskAssigner.class);

  private List<Integer> tasks;
  private ThreadLocalRandom random;

  @Override
  public void configure(Sink sink, List<Integer> tasks) {
    this.random = ThreadLocalRandom.current();
    this.tasks = tasks;
    logger.info(MessageFormat.format("{0} created with tasks {1}", sink, tasks));
  }

  @Override
  public int taskIndex(Row keys) {
    return tasks.get(random.nextInt(tasks.size()));
  }
}
