package io.riffl.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.riffl.config.Execution.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.Test;

public class YamlConfigTests {

  @Test
  void yamlConfigShouldBeLoaded() {
    assertNotNull(
        new YamlConfig(
                ConfigUtils.openFileAsString(new Path("src/test/resources/testApplication.yaml")))
            .getConfig());
    assertNotNull(
        new YamlConfig(
                ConfigUtils.openFileAsString(
                    new Path("src/test/resources/testApplicationNoOverrides.yaml")))
            .getConfig());
  }

  @Test
  void yamlConfigPlaceholdersShouldBeExpanded() {
    Path definitionPath = new Path("src/test/resources/testApplication.yaml");
    var config = new YamlConfig(ConfigUtils.openFileAsString(definitionPath));
    assertEquals(
        "WITH ('path'='${overrides.bucket}${overrides.path}')", config.getCatalogs().get(0).create);
  }

  @Test
  void yamlConfigOverridesShouldBeReturnedAsMap() {
    Path definitionPath = new Path("src/test/resources/testApplication.yaml");
    var config = new YamlConfig(ConfigUtils.openFileAsString(definitionPath));
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
    var config = new YamlConfig(ConfigUtils.openFileAsString(definitionPath));
    assertEquals(Type.FLINK, config.getExecution().getType());
    assertEquals(
        Map.of("execution.checkpointing.mode", "EXACTLY_ONCE", "execution.some.s3.timeout", 60),
        config.getExecution().getProperties());
  }
}
