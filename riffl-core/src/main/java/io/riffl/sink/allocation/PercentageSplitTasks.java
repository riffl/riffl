package io.riffl.sink.allocation;

import io.riffl.config.Sink;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PercentageSplitTasks extends TaskAllocation {
  private static final Logger logger = LoggerFactory.getLogger(PercentageSplitTasks.class);
  private Map<String, List<Integer>> state = new HashMap<>();

  public PercentageSplitTasks(List<Sink> sinks, int parallelism) {
    super(sinks, parallelism);
  }

  @Override
  public List<Integer> getSinkTasks(Sink sink) {
    return state.get(sink.getTable());
  }

  @Override
  public void configure() {
    List<Integer> tasks = IntStream.range(0, getParallelism()).boxed().collect(Collectors.toList());
    var tables = getSinks().stream().map(Sink::getTable).collect(Collectors.toList());
    Collections.shuffle(tasks, new Random(tables.hashCode()));
    List<Iterator<Integer>> taskIterator = new ArrayList<>(List.of(tasks.iterator()));
    this.state =
        getSinks().stream()
            .map(
                sink -> {
                  if (sink.hasParallelism()) {
                    return Map.entry(sink.getTable(), sink.getParallelism());
                  } else {
                    return Map.entry(sink.getTable(), 100);
                  }
                })
            .sorted(Entry.comparingByKey())
            .map(
                table ->
                    Map.entry(
                        table.getKey(),
                        IntStream.range(0, tasks.size() * table.getValue() / 100)
                            .boxed()
                            .map(
                                t -> {
                                  if (!taskIterator.get(0).hasNext()) {
                                    taskIterator.set(0, tasks.iterator());
                                  }
                                  return taskIterator.get(0).next();
                                })
                            .collect(Collectors.toList())))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    logger.info("Task allocation: {}", this.state);
  }
}
