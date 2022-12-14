package io.riffl.utils;

import io.riffl.Riffl;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

public class TableHelper {

  public static ObjectIdentifier getCreateTableIdentifier(
      String stmt, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

    Planner planner = getStreamPlanner(env, tableEnv);

    CreateTableOperation operation = (CreateTableOperation) planner.getParser().parse(stmt).get(0);

    return operation.getTableIdentifier();
  }

  private static Planner getStreamPlanner(
      StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

    CatalogManager catalogManager =
        CatalogManager.newBuilder()
            .classLoader(Riffl.class.getClassLoader())
            .config(tableEnv.getConfig().getConfiguration())
            .defaultCatalog(
                EnvironmentSettings.inStreamingMode().getBuiltInCatalogName(),
                new GenericInMemoryCatalog(
                    EnvironmentSettings.inStreamingMode().getBuiltInCatalogName(),
                    EnvironmentSettings.inStreamingMode().getBuiltInDatabaseName()))
            .build();
    final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    final Executor executor = StreamTableEnvironmentImpl.lookupExecutor(classLoader, env);

    return PlannerFactoryUtil.createPlanner(
        executor,
        tableEnv.getConfig(),
        new ModuleManager(),
        catalogManager,
        new FunctionCatalog(tableEnv.getConfig(), catalogManager, new ModuleManager()));
  }
}
