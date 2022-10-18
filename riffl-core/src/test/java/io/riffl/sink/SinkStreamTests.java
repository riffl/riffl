package io.riffl.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.riffl.config.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

public class SinkStreamTests {

  @Test
  void sinkFlowShouldBeCreatedWithTableIdentifier() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    var sinkStream = new SinkStream(env, tableEnv);
    var tableIdentifier =
        sinkStream.getTableIdentifier(
            new Sink(
                null,
                "default_catalog.default_database.testOutput",
                "select * from testSource",
                null));
    assertEquals(tableIdentifier, "default_catalog.default_database.testOutput");
  }

  @Test
  void sinkFlowShouldBeCreatedWithCreateStmt() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    var create =
        "create table testOutput(id int) WITH ("
            + "  'connector' = 'filesystem',"
            + "  'path' = 'file:///path/to/whatever',"
            + "  'format' = 'raw'"
            + ")";
    var sinkStream = new SinkStream(env, tableEnv);
    var tableIdentifier =
        sinkStream.getTableIdentifier(new Sink(create, null, "select * from testSource", null));

    assertEquals(tableIdentifier, "default_catalog.default_database.testOutput");
  }
}
