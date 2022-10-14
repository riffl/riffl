package io.riffl.config;

import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.text.StringSubstitutor;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtils {

  private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

  public static List<String> listProperty(Object keysObject) {
    try {
      return ((List<?>) keysObject).stream().map(e -> (String) e).collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(
          MessageFormat.format(
              "{0} must be a list of {1}", keysObject, String.class.getCanonicalName()));
    }
  }

  public static Integer integerProperty(String key, Properties properties) {
    return integerProperty(key, properties, null);
  }

  public static Integer integerProperty(String key, Properties properties, Integer defaultValue) {
    var property = properties.get(key);
    try {
      if (property == null) {
        if (defaultValue == null) {
          throw new RuntimeException(MessageFormat.format("{0} must be set", key));
        } else {
          return defaultValue;
        }
      } else {
        return (Integer) property;
      }
    } catch (ClassCastException e) {
      throw new RuntimeException(
          MessageFormat.format("{0} must be {1}", property, Integer.class.getCanonicalName()));
    }
  }

  public static <T extends Enum<T>> T enumProperty(
      Object keysObject, Class<T> enumerator, T defaultValue) {
    try {
      return Enum.valueOf(enumerator, (String) keysObject);
    } catch (Exception e) {
      if (defaultValue != null) {
        logger.info("Failed parsing {} Enum - default used {}", keysObject, defaultValue);
        return defaultValue;
      } else {
        throw new RuntimeException(
            MessageFormat.format("{0} must be a list of {1}", keysObject, enumerator));
      }
    }
  }

  public static String expandOverrides(String content, Map<String, Object> overrides) {
    var ss = new StringSubstitutor(overrides);
    return ss.replace(content);
  }

  public static String expandOverrides(String content, Properties overrides) {
    var map =
        overrides.entrySet().stream()
            .collect(Collectors.toMap(k -> k.getKey().toString(), Entry::getValue));
    return expandOverrides(content, map);
  }

  public static ArrayList<ArrayList<String>> parseKeys(String key, ConfigValue value) {
    if (value.valueType() == ConfigValueType.OBJECT) {
      var obj = (ConfigObject) value;
      var buffer = new ArrayList<ArrayList<String>>();
      for (var v : obj.entrySet()) {
        parseKeys(v.getKey(), v.getValue())
            .forEach(
                part -> {
                  part.add(key);
                  buffer.add(part);
                });
      }
      return buffer;
    } else if (value.valueType() == ConfigValueType.LIST) {
      var list = (ConfigList) value;
      var buffer = new ArrayList<ArrayList<String>>();
      for (var v : list) {
        buffer.addAll(parseKeys(key, v));
      }
      return buffer;
    } else {
      var buffer = new ArrayList<ArrayList<String>>();
      buffer.add(new ArrayList<>(List.of(key)));
      return buffer;
    }
  }

  public static String openFileAsString(Path path) {
    try {
      FileSystem fs = FileSystem.getUnguardedFileSystem(path.toUri());
      try (FSDataInputStream is = fs.open(path)) {
        return new String(is.readAllBytes(), StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String openFileAsString(Path path, Properties overrides) {
    return ConfigUtils.expandOverrides(openFileAsString(path), overrides);
  }
}
