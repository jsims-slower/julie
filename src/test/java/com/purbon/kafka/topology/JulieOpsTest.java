package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.CommandLineInterface.*;
import static com.purbon.kafka.topology.Constants.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.audit.VoidAuditor;
import com.purbon.kafka.topology.backend.BackendState;
import com.purbon.kafka.topology.backend.RedisBackend;
import com.purbon.kafka.topology.exceptions.TopologyParsingException;
import com.purbon.kafka.topology.utils.TestUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JulieOpsTest {

  @Mock TopologyBuilderAdminClient topologyAdminClient;

  @Mock AccessControlProvider accessControlProvider;

  @Mock BindingsBuilderProvider bindingsBuilderProvider;

  @Mock TopicManager topicManager;

  @Mock AccessControlManager accessControlManager;

  @Mock KafkaConnectArtefactManager connectorManager;

  @Mock KSqlArtefactManager ksqlArtefactManager;

  @Mock RedisBackend stateProcessor;

  private final Map<String, String> cliOps = new HashMap<>();
  private final Properties props = new Properties();

  @BeforeEach
  public void before() throws IOException {
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(CLIENT_CONFIG_OPTION, "/fooBar");

    props.put(CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG, "http://foo:8082");
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "");
    props.put(AdminClientConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
  }

  @Test
  public void closeAdminClientTest() throws Exception {
    String fileOrDirPath = TestUtils.getResourceFilename("/descriptor.yaml");

    Configuration builderConfig = new Configuration(cliOps, props);

    try (JulieOps builder =
        JulieOps.build(
            fileOrDirPath,
            builderConfig,
            topologyAdminClient,
            accessControlProvider,
            bindingsBuilderProvider)) {
      assertNotNull(builder);
    }

    verify(topologyAdminClient, times(1)).close();
  }

  @Test
  public void verifyProblematicParametersTest() throws Exception {
    String file = "fileThatDoesNotExist.yaml";
    Configuration builderConfig = new Configuration(cliOps, props);

    assertThrows(
        TopologyParsingException.class,
        () -> {
          try (JulieOps builder =
              JulieOps.build(
                  file,
                  builderConfig,
                  topologyAdminClient,
                  accessControlProvider,
                  bindingsBuilderProvider)) {
            assertNotNull(builder);
          }
        });
  }

  @Test
  public void verifyProblematicParametersTest2() throws Exception {
    String fileOrDirPath = TestUtils.getResourceFilename("/descriptor.yaml");
    Configuration builderConfig = new Configuration(cliOps, props);

    try (JulieOps builder =
        JulieOps.build(
            fileOrDirPath,
            builderConfig,
            topologyAdminClient,
            accessControlProvider,
            bindingsBuilderProvider)) {
      assertNotNull(builder);
    }

    // TODO: This is a poorly written test, and should be revised
    assertThrows(IOException.class, () -> JulieOps.verifyRequiredParameters(fileOrDirPath, cliOps));
  }

  @Test
  public void verifyProblematicParametersTestOK() throws Exception {
    String fileOrDirPath = TestUtils.getResourceFilename("/descriptor.yaml");
    String clientConfigFile = TestUtils.getResourceFilename("/client-config.properties");

    cliOps.put(CLIENT_CONFIG_OPTION, clientConfigFile);

    Configuration builderConfig = new Configuration(cliOps, props);
    try (JulieOps builder =
        JulieOps.build(
            fileOrDirPath,
            builderConfig,
            topologyAdminClient,
            accessControlProvider,
            bindingsBuilderProvider)) {
      assertNotNull(builder);
    }

    // TODO: This test is poorly written, as we never reach here
    JulieOps.verifyRequiredParameters(fileOrDirPath, cliOps);
  }

  @Test
  public void builderRunTestAsFromCLI() throws Exception {
    String fileOrDirPath = TestUtils.getResourceFilename("/descriptor.yaml");
    String clientConfigFile = TestUtils.getResourceFilename("/client-config.properties");

    Map<String, String> config = new HashMap<>();
    config.put(BROKERS_OPTION, "localhost:9092");
    config.put(DRY_RUN_OPTION, "true");
    config.put(QUIET_OPTION, "false");
    config.put(CLIENT_CONFIG_OPTION, clientConfigFile);

    JulieOps builder = JulieOps.build(fileOrDirPath, config);

    builder.setTopicManager(topicManager);
    builder.setAccessControlManager(accessControlManager);
    builder.setConnectorManager(connectorManager);
    builder.setKSqlArtefactManager(ksqlArtefactManager);

    doNothing().when(topicManager).updatePlan(any(ExecutionPlan.class), anyMap());

    doNothing().when(accessControlManager).updatePlan(any(ExecutionPlan.class), anyMap());

    builder.run();
    builder.close();

    verify(topicManager, times(1)).updatePlan(any(ExecutionPlan.class), anyMap());
    verify(accessControlManager, times(1)).updatePlan(any(ExecutionPlan.class), anyMap());
    verify(connectorManager, times(1)).updatePlan(any(ExecutionPlan.class), anyMap());
  }

  @Test
  public void builderRunTestAsFromCLIWithARedisBackend() throws Exception {
    when(stateProcessor.load()).thenReturn(new BackendState());

    String fileOrDirPath = TestUtils.getResourceFilename("/descriptor.yaml");
    String clientConfigFile = TestUtils.getResourceFilename("/client-config-redis.properties");

    Map<String, String> config = new HashMap<>();
    config.put(BROKERS_OPTION, "localhost:9092");
    config.put(DRY_RUN_OPTION, "true");
    config.put(QUIET_OPTION, "false");
    config.put(CLIENT_CONFIG_OPTION, clientConfigFile);

    JulieOps builder = JulieOps.build(fileOrDirPath, config);

    builder.setTopicManager(topicManager);
    builder.setAccessControlManager(accessControlManager);
    builder.setConnectorManager(connectorManager);
    builder.setKSqlArtefactManager(ksqlArtefactManager);

    doNothing().when(topicManager).updatePlan(any(ExecutionPlan.class), anyMap());

    doNothing().when(accessControlManager).updatePlan(any(ExecutionPlan.class), anyMap());

    builder.run(new BackendController(stateProcessor), System.out, new VoidAuditor());
    builder.close();

    verify(stateProcessor, times(1)).createOrOpen();
    verify(topicManager, times(1)).updatePlan(any(ExecutionPlan.class), anyMap());
    verify(accessControlManager, times(1)).updatePlan(any(ExecutionPlan.class), anyMap());
  }

  @Test
  public void builderRunTest() throws Exception {
    String fileOrDirPath = TestUtils.getResourceFilename("/descriptor.yaml");

    Configuration builderConfig = new Configuration(cliOps, props);

    JulieOps builder =
        JulieOps.build(
            fileOrDirPath,
            builderConfig,
            topologyAdminClient,
            accessControlProvider,
            bindingsBuilderProvider);

    builder.setTopicManager(topicManager);
    builder.setAccessControlManager(accessControlManager);
    builder.setConnectorManager(connectorManager);
    builder.setKSqlArtefactManager(ksqlArtefactManager);

    doNothing().when(topicManager).updatePlan(any(ExecutionPlan.class), anyMap());

    doNothing().when(accessControlManager).updatePlan(any(ExecutionPlan.class), anyMap());

    builder.run();
    builder.close();

    verify(topicManager, times(1)).updatePlan(any(ExecutionPlan.class), anyMap());
    verify(accessControlManager, times(1)).updatePlan(any(ExecutionPlan.class), anyMap());
  }

  @Test
  public void builderRunTestAsFromDirectoryWithSchema() throws Exception {
    String fileOrDirPath = TestUtils.getResourceFilename("/dir_with_subdir");

    Configuration builderConfig = new Configuration(cliOps, props);

    JulieOps builder =
        JulieOps.build(
            fileOrDirPath,
            builderConfig,
            topologyAdminClient,
            accessControlProvider,
            bindingsBuilderProvider);

    builder.setTopicManager(topicManager);
    builder.setAccessControlManager(accessControlManager);
    builder.setKSqlArtefactManager(ksqlArtefactManager);

    doNothing().when(topicManager).updatePlan(any(ExecutionPlan.class), anyMap());

    doNothing().when(accessControlManager).updatePlan(any(ExecutionPlan.class), anyMap());

    builder.run();
    builder.close();

    verify(topicManager, times(1)).updatePlan(any(ExecutionPlan.class), anyMap());
    verify(accessControlManager, times(1)).updatePlan(any(ExecutionPlan.class), anyMap());
  }
}
