package io.riffl.utils;

import io.riffl.Riffl;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.delegation.DefaultExecutor;
import org.apache.flink.table.planner.delegation.StreamPlanner;

public class TableHelper {
  public static ObjectIdentifier getCreateTableIdentifier(
      Path path, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

    StreamPlanner planner = getStreamPlanner(env, tableEnv);

    CreateTableOperation operation =
        (CreateTableOperation)
            planner.getParser().parse(FilesystemUtils.openFileAsString(path)).get(0);

    return operation.getTableIdentifier();
  }

  private static StreamPlanner getStreamPlanner(
      StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
    CatalogManager catalogManager =
        CatalogManager.newBuilder()
            .classLoader(Riffl.class.getClassLoader())
            .config(tableEnv.getConfig().getConfiguration())
            .defaultCatalog(
                EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
                new GenericInMemoryCatalog(
                    EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
                    EnvironmentSettings.DEFAULT_BUILTIN_DATABASE))
            .build();

    return new StreamPlanner(
        new DefaultExecutor(env),
        tableEnv.getConfig(),
        new FunctionCatalog(tableEnv.getConfig(), catalogManager, new ModuleManager()),
        catalogManager);
  }
}
