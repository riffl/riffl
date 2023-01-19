package io.riffl.sink.allocation;

import io.riffl.config.Sink;
import java.util.List;

public class PercentageSplitTasksFactory implements TaskAllocationFactory {

  @Override
  public TaskAllocation create(List<Sink> sinks, int parallelism) {
    return new PercentageSplitTasks(sinks, parallelism);
  }
}
