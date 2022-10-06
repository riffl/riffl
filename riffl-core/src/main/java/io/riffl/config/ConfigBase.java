package io.riffl.config;

import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

abstract class ConfigBase {

  private static final String CONFIG_NAME = "name";
  private static final String CONFIG_CATALOGS = "catalogs";

  private static final String CONFIG_DATABASES = "databases";
  private static final String CONFIG_SOURCES = "sources";
  private static final String CONFIG_SOURCE_REBALANCE = "rebalance";
  private static final String CONFIG_SINKS = "sinks";
  private static final String CONFIG_CREATE_URI = "createUri";
  private static final String CONFIG_MAP_URI = "mapUri";
  private static final String CONFIG_QUERY_URI = "queryUri";
  private static final String CONFIG_SINK_DISTRIBUTION = "distribution";
  private static final String CONFIG_SINK_REPARTITION_CLASS_NAME =
      CONFIG_SINK_DISTRIBUTION + ".className";
  private static final String CONFIG_SINK_REPARTITION_PROPERTIES =
      CONFIG_SINK_DISTRIBUTION + ".properties";
  private static final String CONFIG_SINK_REPARTITION_PARALLELISM =
      CONFIG_SINK_DISTRIBUTION + ".parallelism";

  private static final String CONFIG_EXECUTION_OVERRIDES = "configurationOverrides";

  abstract Config getConfig();

  public String getName() {
    return getConfig().getString(CONFIG_NAME);
  }

  public Map<String, String> getExecutionOverrides() {
    return getConfig().getConfig(CONFIG_EXECUTION_OVERRIDES).root().unwrapped().entrySet().stream()
        .map(c -> Map.entry(c.getKey(), c.getValue().toString()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  public List<Catalog> getCatalogs() {
    Config config = getConfig();
    return config.hasPath(CONFIG_CATALOGS)
        ? config.getConfigList(CONFIG_CATALOGS).stream()
            .map(catalog -> new Catalog(catalog.getString(CONFIG_CREATE_URI)))
            .collect(Collectors.toList())
        : List.of();
  }

  public List<Database> getDatabases() {
    Config config = getConfig();
    return config.hasPath(CONFIG_DATABASES)
        ? config.getConfigList(CONFIG_DATABASES).stream()
            .map(catalog -> new Database(catalog.getString(CONFIG_CREATE_URI)))
            .collect(Collectors.toList())
        : List.of();
  }

  public List<Source> getSources() {
    Config config = getConfig();
    return config.getConfigList(CONFIG_SOURCES).stream()
        .map(
            source ->
                new Source(
                    source.getString(CONFIG_CREATE_URI),
                    source.hasPath(CONFIG_MAP_URI) ? source.getString(CONFIG_MAP_URI) : null,
                    source.hasPath(CONFIG_SOURCE_REBALANCE)
                        && source.getBoolean(CONFIG_SOURCE_REBALANCE)))
        .collect(Collectors.toList());
  }

  public List<Sink> getSinks() {
    Config config = getConfig();

    return config.getConfigList(CONFIG_SINKS).stream()
        .map(
            sink -> {
              Properties properties = new Properties();
              if (sink.hasPath(CONFIG_SINK_DISTRIBUTION)
                  && sink.hasPath(CONFIG_SINK_REPARTITION_PROPERTIES)) {
                sink.getConfig(CONFIG_SINK_REPARTITION_PROPERTIES)
                    .entrySet()
                    .forEach(c -> properties.put(c.getKey(), c.getValue().unwrapped()));
              }
              return new Sink(
                  sink.getString(CONFIG_CREATE_URI),
                  sink.hasPath(CONFIG_QUERY_URI) ? sink.getString(CONFIG_QUERY_URI) : null,
                  sink.hasPath(CONFIG_SINK_DISTRIBUTION)
                      ? new Distribution(
                          sink.getString(CONFIG_SINK_REPARTITION_CLASS_NAME),
                          properties,
                          sink.hasPath(CONFIG_SINK_REPARTITION_PARALLELISM)
                              ? sink.getInt(CONFIG_SINK_REPARTITION_PARALLELISM)
                              : null)
                      : null);
            })
        .collect(Collectors.toList());
  }
}
