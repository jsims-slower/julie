package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.CommandLineInterface.*;
import static com.purbon.kafka.topology.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.BackendController;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.ExecutionPlan;
import com.purbon.kafka.topology.TopicManager;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.integration.containerutils.ContainerFactory;
import com.purbon.kafka.topology.integration.containerutils.ContainerTestUtils;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import com.purbon.kafka.topology.integration.containerutils.SchemaRegistryContainer;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import com.purbon.kafka.topology.serdes.TopologySerdes;
import com.purbon.kafka.topology.utils.TestUtils;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.io.File;
import java.io.IOException;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class SchemaRegistryManagerIT {

  @Container
  private static final SaslPlaintextKafkaContainer container =
      ContainerFactory.fetchSaslKafkaContainer(System.getProperty("cp.version"));

  @Container
  private static final SchemaRegistryContainer schemaRegistryContainer =
      new SchemaRegistryContainer(container);

  private TopologySerdes parser;
  private Configuration config;
  private ExecutionPlan plan;

  private SchemaRegistryClient schemaRegistryClient;

  @BeforeEach
  public void configure() throws IOException {
    TestUtils.deleteStateFile();

    parser = new TopologySerdes();

    Properties props = new Properties();
    props.put(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER, "false");
    props.put(ALLOW_DELETE_TOPICS, true);

    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");

    config = new Configuration(cliOps, props);

    this.plan = ExecutionPlan.init(new BackendController(), System.out);

    RestService restService = new RestService(schemaRegistryContainer.getUrl());

    List<SchemaProvider> providers =
        Arrays.asList(
            new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider());
    schemaRegistryClient = new CachedSchemaRegistryClient(restService, 10, providers, null, null);
  }

  @Test
  public void testSchemaSetupForAvroDefaults() throws IOException, RestClientException {
    try (var kafkaAdminClient = ContainerTestUtils.getSaslAdminClient(container)) {
      TopologyBuilderAdminClient adminClient = new TopologyBuilderAdminClient(kafkaAdminClient);

      File file = TestUtils.getResourceFile("/descriptor-schemas-avro.yaml");

      SchemaRegistryManager schemaRegistryManager =
          new SchemaRegistryManager(schemaRegistryClient, file.getAbsolutePath());

      TopicManager topicManager = new TopicManager(adminClient, schemaRegistryManager, config);

      topicManager.updatePlan(parser.deserialise(file), plan);
      plan.run();

      verifySubject(
          "schemas.avro.foo.bar.avro-value",
          "schemas.avro.foo.cat.avro-key",
          "schemas.avro.foo.cat.avro-value");
    }
  }

  @Test
  public void testSchemaSetupForJsonDefaults() throws IOException, RestClientException {
    try (var kafkaAdminClient = ContainerTestUtils.getSaslAdminClient(container)) {
      TopologyBuilderAdminClient adminClient = new TopologyBuilderAdminClient(kafkaAdminClient);

      File file = TestUtils.getResourceFile("/descriptor-schemas-json.yaml");

      SchemaRegistryManager schemaRegistryManager =
          new SchemaRegistryManager(schemaRegistryClient, file.getAbsolutePath());

      TopicManager topicManager = new TopicManager(adminClient, schemaRegistryManager, config);

      topicManager.updatePlan(parser.deserialise(file), plan);
      plan.run();

      verifySubject("schemas.json.foo.foo.json-value");
    }
  }

  @Test
  public void testSchemaSetupForProtoBufDefaults() throws IOException, RestClientException {
    try (var kafkaAdminClient = ContainerTestUtils.getSaslAdminClient(container)) {
      TopologyBuilderAdminClient adminClient = new TopologyBuilderAdminClient(kafkaAdminClient);

      File file = TestUtils.getResourceFile("/descriptor-schemas-proto.yaml");

      SchemaRegistryManager schemaRegistryManager =
          new SchemaRegistryManager(schemaRegistryClient, file.getAbsolutePath());

      TopicManager topicManager = new TopicManager(adminClient, schemaRegistryManager, config);

      topicManager.updatePlan(parser.deserialise(file), plan);
      plan.run();

      verifySubject("schemas.proto.foo.foo.proto-value");
    }
  }

  @Test
  public void testSchemaSetupWithContentInUTF() throws IOException, RestClientException {
    try (var kafkaAdminClient = ContainerTestUtils.getSaslAdminClient(container)) {
      TopologyBuilderAdminClient adminClient = new TopologyBuilderAdminClient(kafkaAdminClient);

      File file = TestUtils.getResourceFile("/descriptor-schemas-utf.yaml");

      SchemaRegistryManager schemaRegistryManager =
          new SchemaRegistryManager(schemaRegistryClient, file.getAbsolutePath());

      TopicManager topicManager = new TopicManager(adminClient, schemaRegistryManager, config);

      topicManager.updatePlan(parser.deserialise(file), plan);
      plan.run();

      String subjectName = "schemas.utf.foo.bar.avro-value";
      verifySubject(subjectName);

      SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subjectName);
      String schema = schemaMetadata.getSchema();

      assertThat(schema).contains("Näme");
      assertThat(schema).contains("Äge");
    }
  }

  private void verifySubject(String... subjects) throws IOException, RestClientException {
    Collection<String> savedSubjects = schemaRegistryClient.getAllSubjects();
    for (String subject : subjects) {
      assertThat(savedSubjects).contains(subject);
    }
  }
}
