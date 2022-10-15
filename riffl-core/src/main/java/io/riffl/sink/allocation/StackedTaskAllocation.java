package io.riffl.sink.allocation;

import io.riffl.config.Sink;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

public class StackedTaskAllocation extends TaskAllocation {

  private static final Logger logger = LoggerFactory.getLogger(StackedTaskAllocation.class);

  private Map<Sink, List<Integer>> state = new HashMap<>();

  public StackedTaskAllocation(List<Sink> sinks, int parallelism) {
    super(sinks, parallelism);
  }

  public List<Integer> getSinkTasks(Sink sink) {
    return state.get(sink);
  }

  public void configure() {
    List<Integer> tasks = IntStream.range(0, getParallelism()).boxed().collect(Collectors.toList());
    Collections.shuffle(tasks, new Random(getSinks().hashCode()));
    List<Iterator<Integer>> taskIterator = new ArrayList<>(List.of(tasks.iterator()));
    logger.info("Tasks {}, hashCode {}", tasks, getSinks().get(0).hashCode());
    this.state =
        getSinks().stream()
            .map(
                s -> {
                  if (s.hasDistribution() && s.getDistribution().hasParallelism()) {
                    return Map.entry(s, s.getDistribution().getParallelism());
                  } else {
                    return Map.entry(s, getParallelism());
                  }
                })
            .sorted(Comparator.comparingInt(Entry::getValue))
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        IntStream.range(0, e.getValue())
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
  }
}
