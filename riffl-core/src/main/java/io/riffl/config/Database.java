package io.riffl.config;

public class Database {
  final String create;

  public Database(String create) {
    this.create = create;
  }

  public String getCreate() {
    return create;
  }
}
