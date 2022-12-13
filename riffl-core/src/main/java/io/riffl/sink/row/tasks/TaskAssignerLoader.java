package io.riffl.sink.row.tasks;

import java.text.MessageFormat;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;

public class TaskAssignerLoader<T> {

  private final Class<T> classType;

  public TaskAssignerLoader(Class<T> classType) {
    this.classType = classType;
  }

  public T load(String className) {
    ServiceLoader<T> taskAssigners = ServiceLoader.load(classType);
    Optional<Provider<T>> taskAssigner =
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
