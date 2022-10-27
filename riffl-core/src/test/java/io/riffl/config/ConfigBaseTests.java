package io.riffl.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValueFactory;
import java.util.List;
import java.util.Map;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

public class ConfigBaseTests {

  Parser parser =
      new FlinkParser(
          StreamExecutionEnvironment.getExecutionEnvironment(),
          StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment()));

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

  private ConfigBase getConfigList(String attribute, List<ConfigObject> items) {
    return new ConfigBase(parser) {
      @Override
      Config getConfig() {
        return ConfigFactory.defaultApplication()
            .withValue(attribute, ConfigValueFactory.fromIterable(items));
      }

      @Override
      Map<String, Object> getConfigAsMap() {
        return null;
      }
    };
  }
}
