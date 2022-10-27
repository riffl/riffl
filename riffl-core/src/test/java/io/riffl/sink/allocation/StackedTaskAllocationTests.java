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
            new Sink("", "sink-2", "", new Distribution("someClass", new Properties()), null));

    var alloc = new StackedTaskAllocation(sinks, 10);
    alloc.configure();

    assertEquals(List.of(0, 7, 8, 1, 5), alloc.getSinkTasks(sinks.get(0)));
    assertEquals(List.of(6, 9, 2, 4, 3, 0, 7, 8, 1, 5), alloc.getSinkTasks(sinks.get(1)));
  }
}
