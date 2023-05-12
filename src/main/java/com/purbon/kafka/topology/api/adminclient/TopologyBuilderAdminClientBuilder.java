package com.purbon.kafka.topology.api.adminclient;

import com.purbon.kafka.topology.Configuration;
import java.io.IOException;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

@Slf4j
@RequiredArgsConstructor
public class TopologyBuilderAdminClientBuilder {

  private final Configuration config;

  public TopologyBuilderAdminClient build() throws IOException {
    Properties props = config.asProperties();
    log.debug(
        "Connecting AdminClient to {}",
        props.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
    TopologyBuilderAdminClient client = new TopologyBuilderAdminClient(AdminClient.create(props));
    if (!config.isDryRun() && !config.doValidate()) {
      client.healthCheck();
    }
    return client;
  }
}
