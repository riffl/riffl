package io.riffl.local;

import io.riffl.Launcher;

public class Riffl extends Launcher {

  public static void main(String[] args) {
    Launcher app = new Riffl();
    app.execute(args);
  }

  @Override
  public boolean isLocal() {
    return true;
  }
}
