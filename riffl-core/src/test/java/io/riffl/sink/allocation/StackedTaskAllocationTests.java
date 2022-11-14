package io.riffl.sink.allocation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.riffl.config.Distribution;
import io.riffl.config.Sink;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class StackedTaskAllocationTests {

  @Test
  void tasksShouldBeAllocated() {
    var sinks =
        List.of(
            new Sink("", "sink-1", "", new Distribution("someClass", new Properties()), 5),
            new Sink("", "sink-2", "", new Distribution("someClass", new Properties()), null),
            new Sink("", "sink-3", "", null, null),
            new Sink("", "sink-4", "", null, 5));

    var alloc = new StackedTaskAllocation(sinks, 10);
    alloc.configure();

    assertEquals(List.of(1, 2, 7, 9, 6), alloc.getSinkTasks(sinks.get(0)));
    assertEquals(List.of(8, 4, 5, 3, 0, 1, 2, 7, 9, 6), alloc.getSinkTasks(sinks.get(1)));
    assertEquals(List.of(8, 4, 5, 3, 0, 1, 2, 7, 9, 6), alloc.getSinkTasks(sinks.get(2)));
    assertEquals(List.of(8, 4, 5, 3, 0), alloc.getSinkTasks(sinks.get(3)));
  }
}
