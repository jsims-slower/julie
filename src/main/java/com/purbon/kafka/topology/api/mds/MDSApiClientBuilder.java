package com.purbon.kafka.topology.api.mds;

import com.purbon.kafka.topology.Configuration;
import java.io.IOException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MDSApiClientBuilder {

  private Configuration config;

  public MDSApiClientBuilder(Configuration config) {
    this.config = config;
  }

  public MDSApiClient build() throws IOException {
    String mdsServer = config.getMdsServer();

    MDSApiClient apiClient = new MDSApiClient(mdsServer, Optional.of(config));
    // Pass Cluster IDS
    apiClient.setKafkaClusterId(config.getKafkaClusterId());
    apiClient.setSchemaRegistryClusterID(config.getSchemaRegistryClusterId());
    apiClient.setConnectClusterID(config.getKafkaConnectClusterId());
    apiClient.setKSqlClusterID(config.getKsqlDBClusterID());

    log.debug("Connecting to an MDS server at {}", mdsServer);
    return apiClient;
  }

  public void configure(Configuration config) {
    this.config = config;
  }
}
