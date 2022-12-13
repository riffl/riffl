package io.riffl.sink.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

public class RowKeyTests {
  @Test
  void rowKeyShouldBeInstantiated() {
    Row row = Row.withNames(RowKind.INSERT);

    row.setField("aaa", "aaaData");
    row.setField("bbb", 0.1d);
    row.setField("ccc", null);

    var keyColumns = List.of("aaa", "bbb", "ccc");

    assertEquals(new RowKey(row, keyColumns), new RowKey(row, keyColumns));
    assertNotEquals(new RowKey(row, List.of("bbb", "ccc")), new RowKey(row, keyColumns));
    assertEquals(new RowKey(row, List.of("bbb", "ccc")), new RowKey(row, List.of("bbb", "ccc")));
  }
}
