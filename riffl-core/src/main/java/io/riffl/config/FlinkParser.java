package io.riffl.config;

import io.riffl.utils.TableHelper;
import java.net.URI;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
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

  @Override
  public URI getCheckpointUri() {
    URI checkpointUri = null;
    if (env.getCheckpointConfig().getCheckpointStorage() instanceof FileSystemCheckpointStorage) {
      var storage = (FileSystemCheckpointStorage) env.getCheckpointConfig().getCheckpointStorage();
      checkpointUri = storage.getCheckpointPath().toUri();
    }
    return checkpointUri;
  }
}
