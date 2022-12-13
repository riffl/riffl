package io.riffl.sink.row;

import io.riffl.config.Sink;
import io.riffl.sink.row.tasks.TaskAssigner;
import io.riffl.sink.row.tasks.TaskAssignerDefaultFactory;
import java.io.Serializable;
import java.util.List;

public class KeyByFactory implements TaskAssignerDefaultFactory, Serializable {

  public KeyByFactory() {}

  @Override
  public TaskAssigner create(Sink sink, List<Integer> tasks, TasksAssignment tasksAssignment) {
    return new KeyByTaskAssigner(sink, tasks, tasksAssignment);
  }
}
