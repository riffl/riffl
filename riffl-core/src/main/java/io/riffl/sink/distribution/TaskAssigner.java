package io.riffl.sink.distribution;

import io.riffl.config.Sink;
import java.util.List;
import org.apache.flink.types.Row;

public interface TaskAssigner {
  void configure(Sink sink, List<Integer> tasks);

  int taskIndex(Row row);
}
