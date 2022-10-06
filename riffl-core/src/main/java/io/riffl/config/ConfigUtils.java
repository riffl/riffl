package io.riffl.config;

import java.text.MessageFormat;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
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
}
