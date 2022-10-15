package io.riffl.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.riffl.config.Execution.Type;
import java.util.Map;
import java.util.Properties;
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
    assertEquals("s3://bucket/path/example/catalog.ddl", config.getCatalogs().get(0).createUri);
  }

  @Test
  void yamlConfigOverridesShouldBeReturnedAsMap() {
    Path definitionPath = new Path("src/test/resources/testApplication.yaml");
    var config = new YamlConfig(ConfigUtils.openFileAsString(definitionPath));
    assertEquals(
        Map.of(
            "overrides.s3.timeout",
            60,
            "overrides.s3.path",
            "path/",
            "overrides.s3a.bucket",
            "s3://bucket"),
        config.getOverrides());
  }

  @Test
  void configFilesShouldLoadedWithOverrides() {
    var props = new Properties();
    props.put("overrides.bucket", "file://bucket/");
    props.put("overrides.path", "path/");

    var result = ConfigUtils.openFileAsString(new Path("src/test/resources/testSink.ddl"), props);

    assertEquals("WITH ('path'='file://bucket/path/')", result);
  }

  @Test
  void executionShouldBeLoaded() {
    Path definitionPath = new Path("src/test/resources/testApplication.yaml");
    var config = new YamlConfig(ConfigUtils.openFileAsString(definitionPath));
    assertEquals(Type.FLINK, config.getExecution().getType());
    assertEquals(
        Map.of(
            "execution.checkpointing.interval", "45s",
            "execution.checkpointing.mode", "EXACTLY_ONCE",
            "execution.some.s3.timeout", 60),
        config.getExecution().getProperties());
  }
}
