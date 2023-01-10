package io.riffl.config;

import com.typesafe.config.Config;
import java.net.URI;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public abstract class ConfigBase {

  private final Parser parser;
  static final String CONFIG_CATALOGS = "catalogs";
  static final String CONFIG_DATABASES = "databases";
  static final String CONFIG_SOURCES = "sources";
  static final String CONFIG_SINKS = "sinks";
  static final String CONFIG_NAME = "name";
  static final String CONFIG_SOURCE_REBALANCE = "rebalance";
  static final String CONFIG_PARALLELISM = "parallelism";
  static final String CONFIG_CREATE_URI = "createUri";
  static final String CONFIG_CREATE = "create";
  static final String CONFIG_TABLE = "table";
  static final String CONFIG_MAP_URI = "mapUri";
  static final String CONFIG_MAP = "map";
  static final String CONFIG_QUERY_URI = "queryUri";
  static final String CONFIG_QUERY = "query";
  static final String CONFIG_SINK_DISTRIBUTION = "distribution";
  protected static final String CONFIG_DELIMITER = ".";
  static final String CONFIG_SINK_DISTRIBUTION_CLASS_NAME =
      CONFIG_SINK_DISTRIBUTION + CONFIG_DELIMITER + "className";
  static final String CONFIG_SINK_DISTRIBUTION_PROPERTIES =
      CONFIG_SINK_DISTRIBUTION + CONFIG_DELIMITER + "properties";

  static final String CONFIG_EXECUTION = "execution";

  static final String CONFIG_EXECUTION_CHECKPOINT_DIR =
      CONFIG_EXECUTION + CONFIG_DELIMITER + "configuration.\"state.checkpoints.dir\"";

  static final String CONFIG_EXECUTION_CONFIGURATION =
      CONFIG_EXECUTION + CONFIG_DELIMITER + "configuration";
  static final String CONFIG_EXECUTION_TYPE = CONFIG_EXECUTION + CONFIG_DELIMITER + "type";

  protected static final String CONFIG_METRICS = "metrics";

  protected static final String CONFIG_METRICS_STORE_URI =
      CONFIG_METRICS + CONFIG_DELIMITER + "storeUri";
  protected static final String CONFIG_METRICS_SKIP_ON_FAILURE =
      CONFIG_METRICS + CONFIG_DELIMITER + "skipOnFailure";

  protected ConfigBase(Parser parser) {
    this.parser = parser;
  }

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
                    new Catalog(loadResourceOrStmt(catalog, CONFIG_CREATE_URI, CONFIG_CREATE)))
            .collect(Collectors.toList())
        : List.of();
  }

  public Execution getExecution() {
    Config config = getConfig();
    var type = Execution.Type.valueOf(config.getString(CONFIG_EXECUTION_TYPE));
    var configuration = new Properties();
    if (config.hasPath(CONFIG_EXECUTION_CONFIGURATION)) {
      configuration.putAll(config.getConfig(CONFIG_EXECUTION_CONFIGURATION).root().unwrapped());
    }
    return new Execution(type, configuration);
  }

  public Metrics getMetrics() {
    Config config = getConfig();
    URI storeUri;
    if (config.hasPath(CONFIG_METRICS_STORE_URI)) {
      storeUri = URI.create(config.getString(CONFIG_METRICS_STORE_URI));
    } else if (config.hasPath(CONFIG_EXECUTION_CHECKPOINT_DIR)) {
      storeUri = URI.create(config.getString(CONFIG_EXECUTION_CHECKPOINT_DIR));
    } else {
      var checkpointUri = parser.getCheckpointUri();
      if (checkpointUri != null) {
        storeUri = checkpointUri;
      } else {
        throw new RuntimeException(
            MessageFormat.format(
                "{0} or checkpointing must be configured.", CONFIG_METRICS_STORE_URI));
      }
    }

    return new Metrics(
        storeUri,
        !config.hasPath(CONFIG_METRICS_SKIP_ON_FAILURE)
            || config.getBoolean(CONFIG_METRICS_SKIP_ON_FAILURE));
  }

  public List<Database> getDatabases() {
    Config config = getConfig();
    return config.hasPath(CONFIG_DATABASES)
        ? config.getConfigList(CONFIG_DATABASES).stream()
            .map(
                database ->
                    new Database(loadResourceOrStmt(database, CONFIG_CREATE_URI, CONFIG_CREATE)))
            .collect(Collectors.toList())
        : List.of();
  }

  public List<Source> getSources() {
    Config config = getConfig();
    return config.getConfigList(CONFIG_SOURCES).stream()
        .map(
            source ->
                new Source(
                    loadResourceOrStmt(source, CONFIG_CREATE_URI, CONFIG_CREATE),
                    source.hasPath(CONFIG_MAP_URI) || source.hasPath(CONFIG_MAP)
                        ? loadResourceOrStmt(source, CONFIG_MAP_URI, CONFIG_MAP)
                        : null,
                    source.hasPath(CONFIG_SOURCE_REBALANCE)
                        && source.getBoolean(CONFIG_SOURCE_REBALANCE)))
        .collect(Collectors.toList());
  }

  public List<Sink> getSinks() {
    Config config = getConfig();

    var sinks =
        config.getConfigList(CONFIG_SINKS).stream()
            .map(
                sink -> {
                  Properties properties = new Properties();
                  if (sink.hasPath(CONFIG_SINK_DISTRIBUTION)
                      && sink.hasPath(CONFIG_SINK_DISTRIBUTION_PROPERTIES)) {
                    sink.getConfig(CONFIG_SINK_DISTRIBUTION_PROPERTIES)
                        .entrySet()
                        .forEach(c -> properties.put(c.getKey(), c.getValue().unwrapped()));
                  }
                  String create = null;
                  if (sink.hasPath(CONFIG_CREATE_URI) || sink.hasPath(CONFIG_CREATE)) {
                    create = loadResourceOrStmt(sink, CONFIG_CREATE_URI, CONFIG_CREATE);
                  }

                  String table;
                  if (create != null && sink.hasPath(CONFIG_TABLE)) {
                    throw new RuntimeException(
                        MessageFormat.format(
                            "Either {0}/{1} or {2} can be set",
                            CONFIG_CREATE_URI, CONFIG_CREATE, CONFIG_TABLE));
                  } else if (create != null) {
                    table = parser.getIdentifier(create);
                  } else if (sink.hasPath(CONFIG_TABLE)) {
                    table = sink.getString(CONFIG_TABLE);
                  } else {
                    throw new RuntimeException(
                        MessageFormat.format(
                            "Either {0}/{1} or {2} must be set",
                            CONFIG_CREATE_URI, CONFIG_CREATE, CONFIG_TABLE));
                  }

                  String query;
                  if (sink.hasPath(CONFIG_QUERY_URI) || sink.hasPath(CONFIG_QUERY)) {
                    query = loadResourceOrStmt(sink, CONFIG_QUERY_URI, CONFIG_QUERY);
                  } else {
                    throw new RuntimeException(
                        MessageFormat.format(
                            "Either {0} or {1} must be set", CONFIG_QUERY_URI, CONFIG_QUERY));
                  }

                  return new Sink(
                      create,
                      table,
                      query,
                      sink.hasPath(CONFIG_SINK_DISTRIBUTION)
                          ? new Distribution(
                              sink.getString(CONFIG_SINK_DISTRIBUTION_CLASS_NAME), properties)
                          : null,
                      sink.hasPath(CONFIG_PARALLELISM) ? sink.getInt(CONFIG_PARALLELISM) : null);
                })
            .collect(Collectors.toList());

    if (sinks.stream().map(Sink::getTable).distinct().count() != sinks.size()) {
      throw new RuntimeException("Sink output tables must be unique");
    }

    return sinks;
  }

  private String loadResource(String uri, Map<String, Object> substitutes) {
    return ConfigUtils.openFileAsString(uri, substitutes);
  }

  private String loadResourceOrStmt(Config config, String configUri, String configStmt) {
    String resource;
    if (config.hasPath(configUri)) {
      resource = loadResource(config.getString(configUri), getConfigAsMap());
    } else {
      resource = config.getString(configStmt);
    }
    return resource;
  }
}
