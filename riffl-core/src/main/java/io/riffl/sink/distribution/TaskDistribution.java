package io.riffl.sink.distribution;

import io.riffl.config.Sink;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaskDistribution implements Serializable {

  private final List<Sink> sinks;
  private final Integer parallelism;
  private Map<Sink, List<Integer>> state = new HashMap<>();

  public TaskDistribution(List<Sink> sinks, Integer parallelism) {
    this.sinks = sinks;
    this.parallelism = parallelism;
  }

  public List<Integer> getTasks(Sink sink) {
    return state.get(sink);
  }

  // Distribute Flink tasks across Sinks
  public void distribute() {
    List<Integer> tasks = IntStream.range(0, parallelism).boxed().collect(Collectors.toList());
    Collections.shuffle(tasks, new Random(sinks.hashCode()));
    AtomicReference<Iterator<Integer>> taskIterator = new AtomicReference<>(tasks.iterator());

    this.state =
        sinks.stream()
            .map(
                s -> {
                  if (s.hasDistribution() && s.getDistribution().hasParallelism()) {
                    return Map.entry(s, s.getDistribution().getParallelism());
                  } else {
                    return Map.entry(s, parallelism);
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
                                  if (!taskIterator.get().hasNext()) {
                                    taskIterator.set(tasks.iterator());
                                  }
                                  return taskIterator.get().next();
                                })
                            .collect(Collectors.toList())))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }
}
