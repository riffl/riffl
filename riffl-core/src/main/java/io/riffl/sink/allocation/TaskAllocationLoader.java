package io.riffl.sink.allocation;

import java.text.MessageFormat;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;

public class TaskAllocationLoader<T> {

  private final Class<T> classType;

  public TaskAllocationLoader(Class<T> classType) {
    this.classType = classType;
  }

  public T load(String className) {
    ServiceLoader<T> TaskAllocators = ServiceLoader.load(classType);
    Optional<Provider<T>> taskAllocation =
        TaskAllocators.stream()
            .filter(ta -> ta.type().getCanonicalName().equals(className))
            .findFirst();
    if (taskAllocation.isPresent()) {
      return taskAllocation.get().get();
    } else {
      throw new RuntimeException(MessageFormat.format("{0} not found", className));
    }
  }
}
