package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.Constants.MDS_VALID_CLUSTER_IDS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.purbon.kafka.topology.api.mds.ClusterIDs;
import com.purbon.kafka.topology.exceptions.ValidationException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ClusterIDsTest {

  private final Map<String, String> cliOps = Collections.singletonMap(BROKERS_OPTION, "");
  private final Properties props = new Properties();

  @Test
  public void shouldRaiseAnExceptionIfAnInvalidClusterIdIsUsed() {
    props.put(MDS_VALID_CLUSTER_IDS_CONFIG + ".0", "kafka-cluster");
    Configuration config = new Configuration(cliOps, props);

    ClusterIDs ids = new ClusterIDs(config);
    assertThrows(ValidationException.class, () -> ids.setKafkaClusterId("foo"));
  }

  @Test
  public void shouldAcceptAValidID() {
    props.put(MDS_VALID_CLUSTER_IDS_CONFIG + ".0", "kafka-cluster");
    Configuration config = new Configuration(cliOps, props);

    ClusterIDs ids = new ClusterIDs(config);
    ids.setKafkaClusterId("kafka-cluster");
  }

  @Test
  public void shouldAnyIdIfTheListIsEmpty() {
    Configuration config = new Configuration(cliOps, props);

    ClusterIDs ids = new ClusterIDs(config);
    ids.setKafkaClusterId("kafka-cluster");
    ids.setKafkaClusterId("kafka-cluster       ");
    ids.setKafkaClusterId("");
  }
}
