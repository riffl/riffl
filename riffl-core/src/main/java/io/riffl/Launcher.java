package io.riffl;

import io.riffl.config.ConfigUtils;
import io.riffl.config.FlinkParser;
import io.riffl.config.YamlConfig;
import io.riffl.sink.SinkStream;
import io.riffl.source.SourceStream;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Launcher {

  public boolean isLocal() {
    return false;
  }

  public void execute(String applicationUri, Properties properties) {
    var parser =
        new FlinkParser(
            StreamExecutionEnvironment.getExecutionEnvironment(),
            StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment()));

    YamlConfig appConfig =
        new YamlConfig(parser, ConfigUtils.openFileAsString(new Path(applicationUri)), properties);
    var executionConfig =
        appConfig.getExecution().getProperties().entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));

    Configuration config = Configuration.fromMap(executionConfig);
    config.set(PipelineOptions.OBJECT_REUSE, true);
    config.set(PipelineOptions.NAME, appConfig.getName());

    StreamExecutionEnvironment env;
    if (isLocal()) {
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
    } else {
      env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    }

    var defaultParallelism = env.getParallelism();

    var sources = new SourceStream(env).build(appConfig.getSources());

    // Reset default parallelism
    env.setParallelism(defaultParallelism);

    new SinkStream(env).build(appConfig, sources).attachAsDataStream();

    try {
      env.execute();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
