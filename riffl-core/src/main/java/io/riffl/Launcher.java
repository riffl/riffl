package io.riffl;

import io.riffl.config.ConfigUtils;
import io.riffl.config.YamlConfig;
import io.riffl.sink.SinkStream;
import io.riffl.source.SourceStream;
import io.riffl.utils.MetaRegistration;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Launcher {

  public boolean isLocal() {
    return false;
  }

  public void execute(String[] args) {

    String application = ParameterTool.fromArgs(args).getRequired("application");

    YamlConfig appConfig = new YamlConfig(ConfigUtils.openFileAsString(new Path(application)));

    Configuration config = Configuration.fromMap(appConfig.getExecutionOverrides());
    config.set(PipelineOptions.OBJECT_REUSE, true);
    config.set(PipelineOptions.NAME, appConfig.getName());

    StreamExecutionEnvironment env;
    if (isLocal()) {
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
    } else {
      env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    }

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    appConfig
        .getCatalogs()
        .forEach(meta -> MetaRegistration.register(tableEnv, meta.getCreateUri()));
    appConfig
        .getDatabases()
        .forEach(meta -> MetaRegistration.register(tableEnv, meta.getCreateUri()));

    new SourceStream(env, tableEnv).build(appConfig.getSources(), appConfig.getOverrides());

    new SinkStream(env, tableEnv).build(appConfig.getSinks(), appConfig.getOverrides()).execute();
  }
}
