package io.riffl.config;

import java.net.URI;

public class Metrics {

  private final URI storeUri;
  private final Boolean skipOnFailure;

  public Metrics(URI storeUri, Boolean skipOnFailure) {
    this.storeUri = storeUri;
    this.skipOnFailure = skipOnFailure;
  }

  public URI getStoreUri() {
    return storeUri;
  }

  public Boolean getSkipOnFailure() {
    return skipOnFailure;
  }
}
