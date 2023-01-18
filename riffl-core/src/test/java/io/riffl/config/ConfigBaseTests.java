package io.riffl.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

public class ConfigBaseTests {

  @Test
  void errorShouldBeThrownIfTableNotUnique() {
    var config =
        getConfigList(
            ConfigBase.CONFIG_SINKS,
            List.of(
                ConfigValueFactory.fromMap(
                    Map.of("id", "id-1", ConfigBase.CONFIG_TABLE, "table-1")),
                ConfigValueFactory.fromMap(
                    Map.of("id", "id-1", ConfigBase.CONFIG_TABLE, "table-1"))));

    assertThrows(RuntimeException.class, config::getSinks);
  }

  @Test
  void sinkTableShouldBeDefinedTableAttribute() {
    ConfigBase config =
        getConfigList(
            ConfigBase.CONFIG_SINKS,
            List.of(
                ConfigValueFactory.fromMap(
                    Map.of(
                        ConfigBase.CONFIG_TABLE,
                        "table-1",
                        ConfigBase.CONFIG_QUERY,
                        "select * from source")),
                ConfigValueFactory.fromMap(
                    Map.of(
                        ConfigBase.CONFIG_TABLE,
                        "table-2",
                        ConfigBase.CONFIG_QUERY,
                        "select * from source"))));

    assertEquals("table-1", config.getSinks().get(0).getTable());
    assertEquals("table-2", config.getSinks().get(1).getTable());
  }

  @Test
  void sinkTableShouldBeLoadedFromFile() {
    ConfigBase config =
        getConfigList(
            ConfigBase.CONFIG_SINKS,
            List.of(
                ConfigValueFactory.fromMap(
                    Map.of(
                        ConfigBase.CONFIG_CREATE_URI,
                        "src/test/resources/testSinkTableName.ddl",
                        ConfigBase.CONFIG_QUERY,
                        "select * from source"))));

    assertEquals("default_catalog.default_database.sink_1", config.getSinks().get(0).getTable());
  }

  @Test
  void sinkCreateShouldBeLoadedBasedOnAttributeType() {
    var ddlFile =
        ConfigUtils.openFileAsString(new Path("src/test/resources/testSinkTableName.ddl"));
    var ddlInline = "CREATE TABLE IF NOT EXISTS sink_2";
    ConfigBase config =
        getConfigList(
            ConfigBase.CONFIG_SINKS,
            List.of(
                ConfigValueFactory.fromMap(
                    Map.of(
                        ConfigBase.CONFIG_CREATE_URI,
                        "src/test/resources/testSinkTableName.ddl",
                        ConfigBase.CONFIG_QUERY,
                        "select * from source")),
                ConfigValueFactory.fromMap(
                    Map.of(
                        ConfigBase.CONFIG_CREATE,
                        ddlInline,
                        ConfigBase.CONFIG_QUERY,
                        "select * from source"))));

    assertEquals(ddlFile, config.getSinks().get(0).getCreate());
    assertEquals("default_catalog.default_database.sink_1", config.getSinks().get(0).getTable());

    assertEquals(ddlInline, config.getSinks().get(1).getCreate());
    assertEquals("default_catalog.default_database.sink_2", config.getSinks().get(1).getTable());
  }

  @Test
  void sinkQueryShouldBeLoadedBasedOnAttributeType() {
    var ddlFile = ConfigUtils.openFileAsString(new Path("src/test/resources/testSinkQuery.ddl"));
    var ddlInline = "SELECT * FROM source";
    ConfigBase config =
        getConfigList(
            ConfigBase.CONFIG_SINKS,
            List.of(
                ConfigValueFactory.fromMap(
                    Map.of(
                        ConfigBase.CONFIG_CREATE,
                        "CREATE TABLE sink_1",
                        ConfigBase.CONFIG_QUERY_URI,
                        "src/test/resources/testSinkQuery.ddl")),
                ConfigValueFactory.fromMap(
                    Map.of(
                        ConfigBase.CONFIG_CREATE,
                        "CREATE TABLE sink_2",
                        ConfigBase.CONFIG_QUERY,
                        ddlInline))));

    assertEquals(ddlFile, config.getSinks().get(0).getQuery());
    assertEquals(ddlInline, config.getSinks().get(1).getQuery());
  }

  @Test
  void sourceCreateShouldBeLoadedFollowingAttributeType() {
    var ddlFile = ConfigUtils.openFileAsString(new Path("src/test/resources/testSource.ddl"));
    var ddlInline = "CREATE TABLE source_1";
    ConfigBase config =
        getConfigList(
            ConfigBase.CONFIG_SOURCES,
            List.of(
                ConfigValueFactory.fromMap(
                    Map.of(ConfigBase.CONFIG_CREATE_URI, "src/test/resources/testSource.ddl")),
                ConfigValueFactory.fromMap(Map.of(ConfigBase.CONFIG_CREATE, ddlInline))));

    assertEquals(ddlFile, config.getSources().get(0).getCreate());
    assertEquals(ddlInline, config.getSources().get(1).getCreate());
  }

  @Test
  void sourceMapShouldBeLoadedFollowingAttributeType() {
    var ddlFile = ConfigUtils.openFileAsString(new Path("src/test/resources/testSourceMap.ddl"));
    var ddlInline = "SELECT * FROM source";
    ConfigBase config =
        getConfigList(
            ConfigBase.CONFIG_SOURCES,
            List.of(
                ConfigValueFactory.fromMap(
                    Map.of(
                        ConfigBase.CONFIG_CREATE,
                        "CREATE TABLE source_1",
                        ConfigBase.CONFIG_MAP_URI,
                        "src/test/resources/testSourceMap.ddl")),
                ConfigValueFactory.fromMap(
                    Map.of(
                        ConfigBase.CONFIG_CREATE,
                        "CREATE TABLE source_1",
                        ConfigBase.CONFIG_MAP,
                        ddlInline))));

    assertEquals(ddlFile, config.getSources().get(0).getMap());
    assertEquals(ddlInline, config.getSources().get(1).getMap());
  }

  @Test
  void databaseCreateShouldBeLoadedFollowingAttributeType() {
    var ddlFile = ConfigUtils.openFileAsString(new Path("src/test/resources/testDatabase.ddl"));
    var ddlInline = "CREATE DATABASE IF NOT EXISTS riffle";
    ConfigBase config =
        getConfigList(
            ConfigBase.CONFIG_DATABASES,
            List.of(
                ConfigValueFactory.fromMap(
                    Map.of(ConfigBase.CONFIG_CREATE_URI, "src/test/resources/testDatabase.ddl")),
                ConfigValueFactory.fromMap(Map.of(ConfigBase.CONFIG_CREATE, ddlInline))));

    assertEquals(ddlFile, config.getDatabases().get(0).getCreate());
    assertEquals(ddlInline, config.getDatabases().get(1).getCreate());
  }

  @Test
  void catalogCreateShouldBeLoadedFollowingAttributeType() {
    var ddlFile = ConfigUtils.openFileAsString(new Path("src/test/resources/testCatalog.ddl"));
    var ddlInline = "CREATE CATALOG custom_catalog";
    ConfigBase config =
        getConfigList(
            ConfigBase.CONFIG_CATALOGS,
            List.of(
                ConfigValueFactory.fromMap(
                    Map.of(ConfigBase.CONFIG_CREATE_URI, "src/test/resources/testCatalog.ddl")),
                ConfigValueFactory.fromMap(Map.of(ConfigBase.CONFIG_CREATE, ddlInline))));

    assertEquals(ddlFile, config.getCatalogs().get(0).getCreate());
    assertEquals(ddlInline, config.getCatalogs().get(1).getCreate());
  }

  @Test
  void metricsConfigurationBeLoadedFromConfigOrCheckpoint() {
    assertThrows(RuntimeException.class, getConfig()::getMetrics);

    // checkpoint
    Configuration flinkConfig = new Configuration();
    flinkConfig.set(
        CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/metricsStore/checkpoint");
    var env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

    Parser parser = new FlinkParser(env, StreamTableEnvironment.create(env));

    var flinkCheckpointConfig = getConfig(parser);

    assertEquals(
        URI.create("file:///tmp/metricsStore/checkpoint"),
        flinkCheckpointConfig.getMetrics().getStoreUri());

    // attribute
    ConfigBase config =
        getConfig(
            Map.of(
                ConfigBase.CONFIG_METRICS_STORE_URI,
                ConfigValueFactory.fromAnyRef("file:///tmp/metricsStore")));

    assertEquals(URI.create("file:///tmp/metricsStore"), config.getMetrics().getStoreUri());

    // execution attribute
    ConfigBase configExecution =
        getConfig(
            Map.of(
                ConfigBase.CONFIG_EXECUTION_CHECKPOINT_DIR,
                ConfigValueFactory.fromAnyRef("file:///tmp/metricsStore")));

    assertEquals(
        URI.create("file:///tmp/metricsStore"), configExecution.getMetrics().getStoreUri());

    // skipOnFailure
    assertTrue(config.getMetrics().getSkipOnFailure());
    ConfigBase configSkipOnFailure =
        getConfig(
            Map.of(
                ConfigBase.CONFIG_METRICS_STORE_URI,
                ConfigValueFactory.fromAnyRef("file:///tmp/metricsStore"),
                ConfigBase.CONFIG_METRICS_SKIP_ON_FAILURE,
                ConfigValueFactory.fromAnyRef(false)));

    assertFalse(configSkipOnFailure.getMetrics().getSkipOnFailure());
  }

  @Test
  void sourceParallelismShouldBeLoadedOrDefault() {
    Configuration flinkConfig = new Configuration();
    flinkConfig.set(CoreOptions.DEFAULT_PARALLELISM, 10);
    var env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

    Parser parser = new FlinkParser(env, StreamTableEnvironment.create(env));

    var configDefaultParallelism =
        getConfig(
            getConfigList(
                    ConfigBase.CONFIG_SOURCES,
                    List.of(
                        ConfigValueFactory.fromMap(
                            Map.of(
                                ConfigBase.CONFIG_CREATE,
                                "CREATE SOURCE",
                                ConfigBase.CONFIG_SOURCE_PARALLELISM,
                                20))))
                .getConfig(),
            parser);

    assertEquals(10, configDefaultParallelism.getSources().get(0).getParallelism());

    var configParamParallelism =
        getConfig(
            getConfigList(
                    ConfigBase.CONFIG_SOURCES,
                    List.of(
                        ConfigValueFactory.fromMap(
                            Map.of(
                                ConfigBase.CONFIG_CREATE,
                                "CREATE SOURCE",
                                ConfigBase.CONFIG_SOURCE_PARALLELISM,
                                5))))
                .getConfig(),
            parser);

    assertEquals(5, configParamParallelism.getSources().get(0).getParallelism());
  }

  private ConfigBase getConfig(Config application, Parser parser) {
    return new ConfigBase(parser) {
      @Override
      Config getConfig() {
        return application;
      }

      @Override
      Map<String, Object> getConfigAsMap() {
        return null;
      }
    };
  }

  private ConfigBase getConfigList(String attribute, List<ConfigObject> items) {
    return getConfig(
        ConfigFactory.defaultApplication()
            .withValue(attribute, ConfigValueFactory.fromIterable(items)));
  }

  private ConfigBase getConfig(Map<String, ConfigValue> config) {
    var result = ConfigFactory.defaultApplication();
    for (var e : config.entrySet()) {
      result = result.withValue(e.getKey(), e.getValue());
    }

    return getConfig(result);
  }

  private ConfigBase getConfig(Parser parser) {
    return getConfig(ConfigFactory.defaultApplication(), parser);
  }

  private ConfigBase getConfig(Config application) {
    Parser parser =
        new FlinkParser(
            StreamExecutionEnvironment.getExecutionEnvironment(),
            StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment()));
    return getConfig(application, parser);
  }

  private ConfigBase getConfig() {
    return getConfig(ConfigFactory.defaultApplication());
  }
}
