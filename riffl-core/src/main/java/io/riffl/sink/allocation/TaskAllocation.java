package io.riffl.sink.allocation;

import io.riffl.config.Sink;
import java.io.Serializable;
import java.util.List;

public abstract class TaskAllocation implements Serializable {

  private final List<Sink> sinks;
  private final int parallelism;

  public TaskAllocation(List<Sink> sinks, int parallelism) {
    this.sinks = sinks;
    this.parallelism = parallelism;
  }

  public List<Sink> getSinks() {
    return sinks;
  }

  public int getParallelism() {
    return parallelism;
  }

  public abstract List<Integer> getSinkTasks(Sink sink);

  // Distribute tasks across Sinks
  public abstract void configure();
}
