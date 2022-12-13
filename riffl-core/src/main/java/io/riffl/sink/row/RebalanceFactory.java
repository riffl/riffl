package io.riffl.sink.row;

import io.riffl.config.Sink;
import io.riffl.sink.row.tasks.TaskAssigner;
import io.riffl.sink.row.tasks.TaskAssignerDefaultFactory;
import java.io.Serializable;
import java.util.List;

public class RebalanceFactory implements TaskAssignerDefaultFactory, Serializable {

  public RebalanceFactory() {}

  @Override
  public TaskAssigner create(Sink sink, List<Integer> tasks, TasksAssignment tasksAssignment) {
    return new RebalanceTaskAssigner(sink, tasks, tasksAssignment);
  }
}
