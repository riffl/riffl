package io.riffl.sink.row;

import io.riffl.config.Sink;
import io.riffl.sink.metrics.Metrics;
import io.riffl.sink.row.tasks.TaskAssigner;
import io.riffl.sink.row.tasks.TaskAssignerMetricsFactory;
import java.io.Serializable;
import java.util.List;

public class DistributeByFactory implements TaskAssignerMetricsFactory, Serializable {

  public DistributeByFactory() {}

  @Override
  public TaskAssigner create(
      Sink sink, List<Integer> tasks, TasksAssignment tasksAssignment, Metrics metrics) {
    return new DistributeByTaskAssigner(sink, tasks, tasksAssignment, metrics);
  }
}
