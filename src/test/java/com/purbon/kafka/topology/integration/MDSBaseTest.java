package com.purbon.kafka.topology.integration;

import com.purbon.kafka.topology.utils.JSON;
import com.purbon.kafka.topology.utils.ZKClient;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MDSBaseTest {

  private final ZKClient zkClient= new ZKClient();

  public void beforeEach() throws IOException, InterruptedException {
    zkClient.connect("localhost");
  }

  protected String getKafkaClusterID() {
    /* TODO: This method is the only reason JulieOps depends on zookeeper,
     * a component that is about to be retired. Figure out a more modern way
     * to get the cluster id, and remove the zookeeper stuff from the code. */
    try {
      String nodeData = zkClient.getNodeData("/cluster/id");
      return JSON.toMap(nodeData).get("id").toString();
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    return "-1";
  }

  protected String getSchemaRegistryClusterID() {
    return "schema-registry";
  }

  protected String getKafkaConnectClusterID() {
    return "connect-cluster";
  }

  protected String getKSqlClusterID() {
    return "ksqldb";
  }
}
