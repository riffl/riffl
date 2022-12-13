package io.riffl.sink.row.tasks;

import io.riffl.config.Sink;
import io.riffl.sink.row.TasksAssignment;
import java.util.List;

public interface TaskAssignerDefaultFactory extends TaskAssignerFactory {
  TaskAssigner create(Sink sink, List<Integer> tasks, TasksAssignment tasksAssignment);
}
