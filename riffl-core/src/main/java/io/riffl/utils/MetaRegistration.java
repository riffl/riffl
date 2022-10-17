package io.riffl.utils;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MetaRegistration {

  public static void register(StreamTableEnvironment tableEnv, String stmt) {
    tableEnv.executeSql(stmt);
  }
}
