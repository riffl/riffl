package io.riffl.config;

import com.typesafe.config.Config;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.flink.core.fs.Path;

abstract class ConfigBase {

  private static final String CONFIG_NAME = "name";
  private static final String CONFIG_CATALOGS = "catalogs";
  private static final String CONFIG_DATABASES = "databases";
  private static final String CONFIG_SOURCES = "sources";
  private static final String CONFIG_SOURCE_REBALANCE = "rebalance";
  private static final String CONFIG_SINKS = "sinks";
  private static final String CONFIG_CREATE_URI = "createUri";
  private static final String CONFIG_TABLE_IDENTIFIER = "tableIdentifier";
  private static final String CONFIG_MAP_URI = "mapUri";
  private static final String CONFIG_QUERY_URI = "queryUri";
  private static final String CONFIG_SINK_DISTRIBUTION = "distribution";
  protected static final String CONFIG_DELIMITER = ".";
  private static final String CONFIG_SINK_REPARTITION_CLASS_NAME =
      CONFIG_SINK_DISTRIBUTION + CONFIG_DELIMITER + "className";
  private static final String CONFIG_SINK_REPARTITION_PROPERTIES =
      CONFIG_SINK_DISTRIBUTION + CONFIG_DELIMITER + "properties";
  private static final String CONFIG_SINK_REPARTITION_PARALLELISM =
      CONFIG_SINK_DISTRIBUTION + CONFIG_DELIMITER + "parallelism";

  private static final String CONFIG_EXECUTION = "execution";

  private static final String CONFIG_EXECUTION_CONFIGURATION =
      CONFIG_EXECUTION + CONFIG_DELIMITER + "configuration";
  private static final String CONFIG_EXECUTION_TYPE = CONFIG_EXECUTION + CONFIG_DELIMITER + "type";

  abstract Config getConfig();

  abstract Map<String, Object> getConfigAsMap();

  public String getName() {
    return getConfig().getString(CONFIG_NAME);
  }

  public List<Catalog> getCatalogs() {
    Config config = getConfig();
    return config.hasPath(CONFIG_CATALOGS)
        ? config.getConfigList(CONFIG_CATALOGS).stream()
            .map(
                catalog ->
                    new Catalog(
                        loadResource(catalog.getString(CONFIG_CREATE_URI), getConfigAsMap())))
            .collect(Collectors.toList())
        : List.of();
  }

  public Execution getExecution() {
    Config config = getConfig();
    var type = Execution.Type.valueOf(config.getString(CONFIG_EXECUTION_TYPE));
    var configuration = new Properties();
    configuration.putAll(config.getConfig(CONFIG_EXECUTION_CONFIGURATION).root().unwrapped());
    return new Execution(type, configuration);
  }

  public List<Database> getDatabases() {
    Config config = getConfig();
    return config.hasPath(CONFIG_DATABASES)
        ? config.getConfigList(CONFIG_DATABASES).stream()
            .map(
                catalog ->
                    new Database(
                        loadResource(catalog.getString(CONFIG_CREATE_URI), getConfigAsMap())))
            .collect(Collectors.toList())
        : List.of();
  }

  public List<Source> getSources() {
    Config config = getConfig();
    return config.getConfigList(CONFIG_SOURCES).stream()
        .map(
            source ->
                new Source(
                    loadResource(source.getString(CONFIG_CREATE_URI), getConfigAsMap()),
                    source.hasPath(CONFIG_MAP_URI)
                        ? loadResource(source.getString(CONFIG_MAP_URI), getConfigAsMap())
                        : null,
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
              String create = null;
              String tableIdentifier = null;
              if (sink.hasPath(CONFIG_CREATE_URI) && sink.hasPath(CONFIG_TABLE_IDENTIFIER)) {
                throw new RuntimeException(
                    MessageFormat.format(
                        "Either {0} or {1} can be set",
                        sink.hasPath(CONFIG_CREATE_URI), sink.hasPath(CONFIG_TABLE_IDENTIFIER)));
              } else if (sink.hasPath(CONFIG_CREATE_URI)) {
                create = loadResource(sink.getString(CONFIG_CREATE_URI), getConfigAsMap());
              } else if (sink.hasPath(CONFIG_TABLE_IDENTIFIER)) {
                tableIdentifier = sink.getString(CONFIG_TABLE_IDENTIFIER);
              } else {
                throw new RuntimeException(
                    MessageFormat.format(
                        "Either {0} or {1} must be set",
                        sink.hasPath(CONFIG_CREATE_URI), sink.hasPath(CONFIG_TABLE_IDENTIFIER)));
              }

              return new Sink(
                  create,
                  tableIdentifier,
                  sink.hasPath(CONFIG_QUERY_URI)
                      ? loadResource(sink.getString(CONFIG_QUERY_URI), getConfigAsMap())
                      : null,
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

  private String loadResource(String uri, Map<String, Object> substitutes) {
    return ConfigUtils.openFileAsString(new Path(uri), substitutes);
  }
}
