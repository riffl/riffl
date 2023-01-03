package io.riffl.config;

import java.net.URI;

interface Parser {
  String getIdentifier(String stmt);

  URI getCheckpointUri();
}
