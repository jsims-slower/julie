package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.Constants.MDS_VALID_CLUSTER_IDS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.purbon.kafka.topology.api.mds.ClusterIDs;
import com.purbon.kafka.topology.exceptions.ValidationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClusterIDsTest {

  private final Map<String, String> cliOps = new HashMap<>();
  private final Properties props = new Properties();

  @BeforeEach
  public void before() {
    cliOps.put(BROKERS_OPTION, "");
  }

  @Test
  public void shouldRaiseAnExceptionIfAnInvalidClusterIdIsUsed() {
    props.put(MDS_VALID_CLUSTER_IDS_CONFIG + ".0", "kafka-cluster");
    Configuration config = new Configuration(cliOps, props);

    ClusterIDs ids = new ClusterIDs(Optional.of(config));
    assertThrows(ValidationException.class, () -> ids.setKafkaClusterId("foo"));
  }

  @Test
  public void shouldAcceptAValidID() {
    props.put(MDS_VALID_CLUSTER_IDS_CONFIG + ".0", "kafka-cluster");
    Configuration config = new Configuration(cliOps, props);

    ClusterIDs ids = new ClusterIDs(Optional.of(config));
    ids.setKafkaClusterId("kafka-cluster");
  }

  @Test
  public void shouldAnyIdIfTheListIsEmpty() {
    Configuration config = new Configuration(cliOps, props);

    ClusterIDs ids = new ClusterIDs(Optional.of(config));
    ids.setKafkaClusterId("kafka-cluster");
    ids.setKafkaClusterId("kafka-cluster       ");
    ids.setKafkaClusterId("");
  }
}
