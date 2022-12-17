package io.riffl.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.riffl.config.Execution.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

public class YamlConfigTests {

  Parser parser =
      new FlinkParser(
          StreamExecutionEnvironment.getExecutionEnvironment(),
          StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment()));

  @Test
  void yamlConfigShouldBeLoaded() {
    assertNotNull(
        new YamlConfig(
                parser,
                ConfigUtils.openFileAsString(new Path("src/test/resources/testApplication.yaml")),
                new Properties())
            .getConfig());
    assertNotNull(
        new YamlConfig(
                parser,
                ConfigUtils.openFileAsString(
                    new Path("src/test/resources/testApplicationNoOverrides.yaml")),
                new Properties())
            .getConfig());
  }

  @Test
  void yamlConfigPlaceholdersShouldBeExpanded() {
    Path definitionPath = new Path("src/test/resources/testApplication.yaml");
    var config =
        new YamlConfig(parser, ConfigUtils.openFileAsString(definitionPath), new Properties());
    assertEquals(
        "WITH ('path'='${overrides.bucket}${overrides.path}')", config.getCatalogs().get(0).create);
  }

  @Test
  void yamlConfigOverridesShouldBeReturnedAsMap() {
    Path definitionPath = new Path("src/test/resources/testApplication.yaml");
    var config =
        new YamlConfig(parser, ConfigUtils.openFileAsString(definitionPath), new Properties());
    assertEquals(
        Map.of(
            "name", "Riffl application",
            "overrides.s3.timeout", 60,
            "overrides.s3.path", "src/test/resources/",
            "overrides.s3a.bucket", "./",
            "execution.type", "FLINK",
            "execution.configuration.execution.checkpointing.mode", "EXACTLY_ONCE",
            "execution.configuration.execution.some.s3.timeout", "${overrides.s3.timeout}"),
        config.getConfigAsMap());
  }

  @Test
  void configFilesShouldLoadedWithOverrides() {
    var substitutes = new HashMap<String, Object>();
    substitutes.put("overrides.bucket", "file://bucket/");
    substitutes.put("overrides.path", "path/");

    var result =
        ConfigUtils.openFileAsString(new Path("src/test/resources/testSink.ddl"), substitutes);

    assertEquals("WITH ('path'='file://bucket/path/')", result);
  }

  @Test
  void executionShouldBeLoaded() {
    Path definitionPath = new Path("src/test/resources/testApplication.yaml");
    var config =
        new YamlConfig(parser, ConfigUtils.openFileAsString(definitionPath), new Properties());
    assertEquals(Type.FLINK, config.getExecution().getType());
    assertEquals(
        Map.of("execution.checkpointing.mode", "EXACTLY_ONCE", "execution.some.s3.timeout", 60),
        config.getExecution().getProperties());
  }

  @Test
  void entriesShouldBeOverriddenByProperties() {
    Path definitionPath = new Path("src/test/resources/testApplication.yaml");

    var properties = new Properties();
    properties.put("execution.configuration.\"execution.checkpointing.mode\"", "AT_LEAST_ONCE");
    properties.put("overrides.s3.timeout", 50);
    var config = new YamlConfig(parser, ConfigUtils.openFileAsString(definitionPath), properties);

    assertEquals(Type.FLINK, config.getExecution().getType());
    assertEquals(
        Map.of("execution.checkpointing.mode", "AT_LEAST_ONCE", "execution.some.s3.timeout", 50),
        config.getExecution().getProperties());
  }
}
