package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.CommandLineInterface.*;
import static com.purbon.kafka.topology.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.purbon.kafka.topology.BackendController;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.ExecutionPlan;
import com.purbon.kafka.topology.TestTopologyBuilder;
import com.purbon.kafka.topology.TopicManager;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.exceptions.RemoteValidationException;
import com.purbon.kafka.topology.integration.containerutils.ContainerFactory;
import com.purbon.kafka.topology.integration.containerutils.ContainerTestUtils;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import com.purbon.kafka.topology.utils.TestUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class TopicManagerIT {

  @Container
  private static SaslPlaintextKafkaContainer container =
      ContainerFactory.fetchSaslKafkaContainer(System.getProperty("cp.version"));

  private TopicManager topicManager;
  private AdminClient kafkaAdminClient;

  private ExecutionPlan plan;

  @BeforeEach
  public void before() throws IOException {
    TestUtils.deleteStateFile();

    kafkaAdminClient = ContainerTestUtils.getSaslAdminClient(container);
    TopologyBuilderAdminClient adminClient = new TopologyBuilderAdminClient(kafkaAdminClient);

    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final SchemaRegistryManager schemaRegistryManager =
        new SchemaRegistryManager(schemaRegistryClient, System.getProperty("user.dir"));
    this.plan = ExecutionPlan.init(new BackendController(), System.out);

    Properties props = new Properties();
    props.put(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER, "false");
    props.put(ALLOW_DELETE_TOPICS, true);

    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");

    Configuration config = new Configuration(cliOps, props);

    this.topicManager = new TopicManager(adminClient, schemaRegistryManager, config);
  }

  @AfterEach
  public void after() {
    kafkaAdminClient.close();
  }

  @Test
  public void testTopicCreation() throws ExecutionException, InterruptedException, IOException {

    Topology topology = new TopologyImpl();
    topology.setContext("testTopicCreation");
    Project project = new ProjectImpl("project");
    topology.addProject(project);

    HashMap<String, String> config = new HashMap<>();
    config.put(TopicManager.NUM_PARTITIONS, "1");
    config.put(TopicManager.REPLICATION_FACTOR, "1");

    Topic topicA = new Topic("topicA", config);
    project.addTopic(topicA);

    config = new HashMap<>();
    config.put(TopicManager.NUM_PARTITIONS, "1");
    config.put(TopicManager.REPLICATION_FACTOR, "1");

    Topic topicB = new Topic("topicB", config);
    project.addTopic(topicB);

    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopics(topicA.toString(), topicB.toString());
  }

  @Test
  public void topicManagerShouldDetectDeletedTopicsBetweenRuns() throws IOException {

    TopologyBuilderAdminClient adminClient = new TopologyBuilderAdminClient(kafkaAdminClient);

    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final SchemaRegistryManager schemaRegistryManager =
        new SchemaRegistryManager(schemaRegistryClient, System.getProperty("user.dir"));

    Properties props = new Properties();
    props.put(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER, "false");
    props.put(ALLOW_DELETE_TOPICS, true);
    props.put(JULIE_VERIFY_STATE_SYNC, true);

    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");

    Configuration config = new Configuration(cliOps, props);

    this.topicManager = new TopicManager(adminClient, schemaRegistryManager, config);

    Topic topic1 = new Topic("topic1");
    Topic topic2 = new Topic("topic2");

    var topology =
        TestTopologyBuilder.createProject().addTopic(topic1).addTopic(topic2).buildTopology();

    topicManager.updatePlan(topology, plan);
    plan.run();

    adminClient.deleteTopics(Collections.singletonList("ctx.project.topic1"));
    assertThrows(RemoteValidationException.class, () -> topicManager.updatePlan(topology, plan));
  }

  @Test
  public void testTopicCreationWithDefaultTopicConfigs()
      throws IOException, ExecutionException, InterruptedException {

    Topology topology = new TopologyImpl();
    topology.setContext("testTopicCreationWithDefaultTopicConfigs");
    Project project = new ProjectImpl("project");
    topology.addProject(project);

    Topic topicA = new Topic("topicA");
    project.addTopic(topicA);
    Topic topicB = new Topic("topicB");
    project.addTopic(topicB);

    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopics(topicA.toString(), topicB.toString());
  }

  @Test
  public void testTopicCreationWithFalseConfig() throws IOException {
    HashMap<String, String> config = new HashMap<>();
    config.put("num.partitions", "1");
    config.put("replication.factor", "1");
    config.put("banana", "bar");

    Project project = new ProjectImpl("project");
    Topology topology = new TopologyImpl();
    topology.setContext("testTopicCreationWithFalseConfig");
    topology.addProject(project);

    Topic topicA = new Topic("topicA", config);
    project.addTopic(topicA);

    topicManager.updatePlan(topology, plan);
    assertThrows(IOException.class, () -> plan.run());
  }

  @Test
  public void testTopicCreationWithChangedTopology()
      throws ExecutionException, InterruptedException, IOException {
    HashMap<String, String> config = new HashMap<>();
    config.put(TopicManager.NUM_PARTITIONS, "1");
    config.put(TopicManager.REPLICATION_FACTOR, "1");

    Topology topology = new TopologyImpl();
    topology.setContext("testTopicCreationWithChangedTopology");
    Project project = new ProjectImpl("project");
    topology.addProject(project);

    Topic topicA = new Topic("topicA", config);
    project.addTopic(topicA);

    config = new HashMap<>();
    config.put(TopicManager.NUM_PARTITIONS, "1");
    config.put(TopicManager.REPLICATION_FACTOR, "1");

    Topic topicB = new Topic("topicB", config);
    project.addTopic(topicB);

    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopics(topicA.toString(), topicB.toString());

    Topology upTopology = new TopologyImpl();
    upTopology.setContext("testTopicCreationWithChangedTopology");
    Project upProject = new ProjectImpl("bar");
    upTopology.addProject(upProject);

    config = new HashMap<>();
    config.put(TopicManager.NUM_PARTITIONS, "1");
    config.put(TopicManager.REPLICATION_FACTOR, "1");

    topicA = new Topic("topicA", config);
    upProject.addTopic(topicA);

    config = new HashMap<>();
    config.put(TopicManager.NUM_PARTITIONS, "1");
    config.put(TopicManager.REPLICATION_FACTOR, "1");

    topicB = new Topic("topicB", config);
    upProject.addTopic(topicB);

    plan.getActions().clear();
    topicManager.updatePlan(upTopology, plan);
    plan.run();

    verifyTopics(topicA.toString(), topicB.toString());
  }

  @Test
  public void testTopicDelete() throws ExecutionException, InterruptedException, IOException {

    Project project = new ProjectImpl("project");
    Topic topicA = new Topic("topicA", buildDummyTopicConfig());
    project.addTopic(topicA);

    Topic topicB = new Topic("topicB", buildDummyTopicConfig());
    project.addTopic(topicB);

    String internalTopic = createInternalTopic();

    Topology topology = new TopologyImpl();
    topology.setContext("testTopicDelete-test");
    topology.addProject(project);

    topicManager.updatePlan(topology, plan);
    plan.run();

    Topic topicC = new Topic("topicC", buildDummyTopicConfig());

    topology = new TopologyImpl();
    topology.setContext("testTopicDelete-test");

    project = new ProjectImpl("project");
    project.addTopic(topicA);
    project.addTopic(topicC);

    topology.addProject(project);

    plan.getActions().clear();
    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopics(2, topicA.toString(), internalTopic, topicC.toString());
  }

  private String createInternalTopic() {

    String topic = "_internal-topic";
    NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

    try {
      kafkaAdminClient.createTopics(Collections.singleton(newTopic)).all().get();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return topic;
  }

  @Test
  public void testTopicCreationWithConfig()
      throws ExecutionException, InterruptedException, IOException {

    Topology topology = new TopologyImpl();
    topology.setContext("testTopicCreationWithConfig-test");
    Project project = new ProjectImpl("project");
    topology.addProject(project);

    HashMap<String, String> config = buildDummyTopicConfig();
    config.put("retention.bytes", "104857600"); // set the retention.bytes per partition to 100mb
    Topic topicA = new Topic("topicA", config);
    project.addTopic(topicA);

    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopicConfiguration(topicA.toString(), config);
  }

  @Test
  public void testTopicConfigUpdate() throws ExecutionException, InterruptedException, IOException {

    HashMap<String, String> config = buildDummyTopicConfig();
    config.put("retention.bytes", "104857600"); // set the retention.bytes per partition to 100mb
    config.put("segment.bytes", "104857600");

    Topology topology = new TopologyImpl();
    topology.setContext("testTopicConfigUpdate-test");
    Project project = new ProjectImpl("project");
    topology.addProject(project);

    Topic topicA = new Topic("topicA", config);
    project.addTopic(topicA);

    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopicConfiguration(topicA.toString(), config);

    config = buildDummyTopicConfig();
    config.put("retention.bytes", "104");
    topicA = new Topic("topicA", config);
    project.setTopics(Collections.singletonList(topicA));
    topology.setContext("testTopicConfigUpdate-test");
    topology.setProjects(Collections.singletonList(project));

    plan.getActions().clear();
    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopicConfiguration(topicA.toString(), config, Collections.singletonList("segment.bytes"));
  }

  private void verifyTopicConfiguration(String topic, HashMap<String, String> config)
      throws ExecutionException, InterruptedException {
    verifyTopicConfiguration(topic, config, new ArrayList<>());
  }

  private void verifyTopicConfiguration(
      String topic, HashMap<String, String> config, List<String> removedConfigs)
      throws ExecutionException, InterruptedException {

    ConfigResource resource = new ConfigResource(Type.TOPIC, topic);
    Collection<ConfigResource> resources = Collections.singletonList(resource);

    Map<ConfigResource, Config> configs = kafkaAdminClient.describeConfigs(resources).all().get();

    assertThat(configs.get(resource))
        .isNotNull()
        .satisfies(
            topicConfig ->
                assertThat(topicConfig.entries())
                    .isNotEmpty()
                    .filteredOn(Predicate.not(ConfigEntry::isDefault))
                    .allSatisfy(
                        entry -> {
                          if (config.get(entry.name()) != null)
                            assertEquals(config.get(entry.name()), entry.value());
                          assertThat(removedConfigs).doesNotContain(entry.name());
                        }));
  }

  private HashMap<String, String> buildDummyTopicConfig() {
    HashMap<String, String> config = new HashMap<>();
    config.put(TopicManager.NUM_PARTITIONS, "1");
    config.put(TopicManager.REPLICATION_FACTOR, "1");
    return config;
  }

  private void verifyTopics(String... topics) throws ExecutionException, InterruptedException {
    verifyTopics(topics.length, topics);
  }

  private void verifyTopics(int topicsCount, String... topics)
      throws ExecutionException, InterruptedException {

    Set<String> topicNames = kafkaAdminClient.listTopics().names().get();
    assertThat(topicNames).contains(topics);
    assertThat(topicNames)
        .describedAs("Internal topics not found")
        .anyMatch(topic -> topic.startsWith("_"));
    Set<String> nonInternalTopics =
        topicNames.stream().filter(topic -> !topic.startsWith("_")).collect(Collectors.toSet());

    assertThat(nonInternalTopics).hasSizeGreaterThanOrEqualTo(topicsCount);
  }
}
