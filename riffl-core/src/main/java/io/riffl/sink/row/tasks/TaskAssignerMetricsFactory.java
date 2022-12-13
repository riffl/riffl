package io.riffl.sink.row.tasks;

import io.riffl.config.Sink;
import io.riffl.sink.metrics.Metrics;
import io.riffl.sink.row.TasksAssignment;
import java.util.List;

public interface TaskAssignerMetricsFactory extends TaskAssignerFactory {
  TaskAssigner create(
      Sink sink, List<Integer> tasks, TasksAssignment tasksAssignment, Metrics metrics);
}
