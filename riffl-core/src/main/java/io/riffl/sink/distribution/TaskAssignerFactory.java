package io.riffl.sink.distribution;

import java.text.MessageFormat;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;

public class TaskAssignerFactory {
  public static TaskAssigner load(String className) {
    ServiceLoader<TaskAssigner> taskAssigners = ServiceLoader.load(TaskAssigner.class);
    Optional<Provider<TaskAssigner>> taskAssigner =
        taskAssigners.stream()
            .filter(ta -> ta.type().getCanonicalName().equals(className))
            .findFirst();
    if (taskAssigner.isPresent()) {
      return taskAssigner.get().get();
    } else {
      throw new RuntimeException(MessageFormat.format("{0} not found", className));
    }
  }
}
