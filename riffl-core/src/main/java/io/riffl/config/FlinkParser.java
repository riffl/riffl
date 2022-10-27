package io.riffl.config;

import io.riffl.utils.TableHelper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkParser implements Parser {

  private final StreamExecutionEnvironment env;
  private final StreamTableEnvironment tableEnv;

  public FlinkParser(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
    this.env = env;
    this.tableEnv = tableEnv;
  }

  @Override
  public String getIdentifier(String stmt) {
    return TableHelper.getCreateTableIdentifier(stmt, env, tableEnv).asSummaryString();
  }
}
