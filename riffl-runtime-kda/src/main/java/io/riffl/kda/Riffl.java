package io.riffl.kda;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import io.riffl.Launcher;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Properties;

public class Riffl extends Launcher {

  private static final String KDA_RIFFL_GROUP_ID = "RifflConfigProperties";
  private static final String KDA_OVERRIDE_GROUP_ID = "OverrideConfigProperties";
  private static final String KDA_APPLICATION_PROPERTY = "application";

  public static void main(String[] args) throws IOException {
    Launcher app = new Riffl();
    var applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
    var inputProperties = applicationProperties.get(KDA_RIFFL_GROUP_ID);

    if (inputProperties != null && inputProperties.getProperty(KDA_APPLICATION_PROPERTY) != null) {
      var overridesProperties = new Properties();
      if (applicationProperties.get(KDA_OVERRIDE_GROUP_ID) != null) {
        overridesProperties.putAll(applicationProperties.get(KDA_OVERRIDE_GROUP_ID));
      }
      app.execute(inputProperties.getProperty(KDA_APPLICATION_PROPERTY), overridesProperties);
    } else {
      throw new RuntimeException(
          MessageFormat.format(
              "{}.{} must be defined", KDA_RIFFL_GROUP_ID, KDA_APPLICATION_PROPERTY));
    }
  }
}
