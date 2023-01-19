package io.riffl.sink.allocation;

import io.riffl.config.Sink;
import java.util.List;

public interface TaskAllocationFactory {
  TaskAllocation create(List<Sink> sinks, int parallelism);
}
