package com.purbon.kafka.topology.integration.containerutils;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class ContainerFactory {

  public static SaslPlaintextKafkaContainer fetchSaslKafkaContainer(String version) {
    log.debug("Fetching SASL Kafka Container with version={}", version);
    if (version == null || version.isEmpty()) {
      return new SaslPlaintextKafkaContainer();
    } else {
      DockerImageName containerImage =
          DockerImageName.parse("confluentinc/cp-kafka").withTag(version);
      return new SaslPlaintextKafkaContainer(containerImage);
    }
  }
}
