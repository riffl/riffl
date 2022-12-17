package io.riffl.local;

import io.riffl.Launcher;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;

public class Riffl extends Launcher {

  public static void main(String[] args) {
    Launcher app = new Riffl();
    String applicationUri = ParameterTool.fromArgs(args).getRequired("application");
    app.execute(applicationUri, new Properties());
  }

  @Override
  public boolean isLocal() {
    return true;
  }
}
