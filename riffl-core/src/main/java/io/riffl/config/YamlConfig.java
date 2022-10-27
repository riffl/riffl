package io.riffl.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class YamlConfig extends ConfigBase {

  private final Parser parser;

  private static final String STRING_DELIMITER = "\"";

  private final String contentYaml;

  public YamlConfig(Parser parser, String contentYaml) {
    super(parser);
    this.parser = parser;
    this.contentYaml = contentYaml;
  }

  protected Config getConfig() {
    return parse(ConfigUtils.replaceSubstitutes(contentYaml, getConfigAsMap()));
  }

  @Override
  protected Map<String, Object> getConfigAsMap() {
    var config = parse(contentYaml);
    return ConfigUtils.parseKeys(null, config.root()).stream()
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
                k -> k.replace(STRING_DELIMITER, ""), k -> config.getValue(k).unwrapped()));
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
}
