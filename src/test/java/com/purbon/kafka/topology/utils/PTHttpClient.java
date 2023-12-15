package com.purbon.kafka.topology.utils;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.clients.JulieHttpClient;
import java.io.IOException;

public class PTHttpClient extends JulieHttpClient {

  public PTHttpClient(String server, Configuration config) throws IOException {
    super(server, config);
  }
}
