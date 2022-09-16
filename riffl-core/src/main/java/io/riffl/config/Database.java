package io.riffl.config;

public class Database {
  final String createUri;

  public Database(String createUri) {
    this.createUri = createUri;
  }

  public String getCreateUri() {
    return createUri;
  }
}
