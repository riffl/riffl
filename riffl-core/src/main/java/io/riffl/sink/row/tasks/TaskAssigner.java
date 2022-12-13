package io.riffl.sink.row.tasks;

import io.riffl.sink.row.RowKey;
import org.apache.flink.types.Row;

public interface TaskAssigner {
  int getTask(Row row, RowKey key);

  RowKey getKey(Row row);
}
