package io.riffl.sink.tasks;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.riffl.sink.row.DistributeByFactory;
import io.riffl.sink.row.tasks.TaskAssignerFactory;
import io.riffl.sink.row.tasks.TaskAssignerLoader;
import io.riffl.sink.row.tasks.TaskAssignerMetricsFactory;
import org.junit.jupiter.api.Test;

public class TaskAssignerLoaderTests {

  @Test
  void tasksAssignedShouldBeInstantiated() {
    var loader = new TaskAssignerLoader<>(TaskAssignerFactory.class);

    var taskAssigner = loader.load(DistributeByFactory.class.getCanonicalName());
    assertTrue(taskAssigner instanceof TaskAssignerMetricsFactory);
  }
}
