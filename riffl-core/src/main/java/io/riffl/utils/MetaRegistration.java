package io.riffl.utils;

import io.riffl.config.ConfigUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MetaRegistration {

  public static void register(StreamTableEnvironment tableEnv, String stmtUri) {
    Path definitionPath = new Path(stmtUri);
    tableEnv.executeSql(ConfigUtils.openFileAsString(definitionPath));
  }
}
