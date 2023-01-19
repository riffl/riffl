package io.riffl.sink.allocation;

import io.riffl.config.Sink;
import java.util.List;

public class StackedTasksFactory implements TaskAllocationFactory {

  @Override
  public TaskAllocation create(List<Sink> sinks, int parallelism) {
    return new StackedTasks(sinks, parallelism);
  }
}
