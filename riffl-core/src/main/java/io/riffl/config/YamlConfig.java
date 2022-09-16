package io.riffl.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class YamlConfig extends ConfigBase {
  private final Config config;

  public YamlConfig(String fileName) {
    ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    ObjectMapper jsonWriter = new ObjectMapper();
    try {
      String json =
          jsonWriter.writeValueAsString(
              yamlReader.readValue(Files.readString(Path.of(fileName)), Object.class));

      config = ConfigFactory.parseString(json);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Config getConfig() {
    return config;
  }
}
