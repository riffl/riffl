package io.riffl.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class YamlConfig extends ConfigBase {

  private static final String STRING_DELIMITER = "\"";
  private final Config config;

  public YamlConfig(String contentYaml) {

    var overrides = parseOverrides(contentYaml);
    var parsedConfig = parse(ConfigUtils.expandOverrides(contentYaml, overrides));

    for (var override : overrides.entrySet()) {
      parsedConfig =
          parsedConfig.withValue(
              override.getKey(), ConfigValueFactory.fromAnyRef(override.getValue()));
    }
    this.config = parsedConfig;
  }

  protected Config getConfig() {
    return config;
  }

  private Config parse(String contentYaml) {
    try {
      ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
      ObjectMapper jsonWriter = new ObjectMapper();
      String hocon = jsonWriter.writeValueAsString(yamlReader.readValue(contentYaml, Object.class));
      return ConfigFactory.parseString(hocon);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, Object> parseOverrides(String contentYaml) {
    var parsedConfig = parse(contentYaml);
    if (parsedConfig.hasPath(CONFIG_OVERRIDES)) {
      return ConfigUtils.parseKeys(
              CONFIG_OVERRIDES, parsedConfig.getConfig(CONFIG_OVERRIDES).root())
          .stream()
          .map(
              key -> {
                Collections.reverse(key);
                var quoted =
                    key.stream()
                        .map(keyPart -> STRING_DELIMITER + keyPart + STRING_DELIMITER)
                        .collect(Collectors.toList());
                return String.join(CONFIG_DELIMITER, quoted);
              })
          .distinct()
          .collect(
              Collectors.toMap(
                  k -> k.replace(STRING_DELIMITER, ""), k -> parsedConfig.getValue(k).unwrapped()));
    } else {
      return Map.of();
    }
  }
}
