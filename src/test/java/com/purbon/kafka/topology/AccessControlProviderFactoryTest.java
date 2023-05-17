package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.api.mds.MDSApiClient;
import com.purbon.kafka.topology.api.mds.MDSApiClientBuilder;
import com.purbon.kafka.topology.roles.RBACProvider;
import com.purbon.kafka.topology.roles.SimpleAclsProvider;
import com.purbon.kafka.topology.utils.BasicAuth;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AccessControlProviderFactoryTest {

  @Mock TopologyBuilderAdminClient adminClient;

  @Mock MDSApiClientBuilder mdsApiClientBuilder;

  @Mock MDSApiClient mdsApiClient;

  private final Map<String, String> cliOps = new HashMap<>();
  private final Properties props = new Properties();

  @BeforeEach
  public void before() {
    cliOps.put(BROKERS_OPTION, "");
  }

  @AfterEach
  public void after() {
    verifyNoMoreInteractions(adminClient, mdsApiClientBuilder, mdsApiClient);
  }

  @Test
  public void testRBACConfig() throws IOException {

    props.put(ACCESS_CONTROL_IMPLEMENTATION_CLASS, "com.purbon.kafka.topology.roles.RBACProvider");
    props.put(MDS_SERVER, "http://localhost:8090");
    props.put(MDS_USER_CONFIG, "alice");
    props.put(MDS_PASSWORD_CONFIG, "alice-secret");
    props.put(MDS_KAFKA_CLUSTER_ID_CONFIG, "UtBZ3rTSRtypmmkAL1HbHw");

    Configuration config = new Configuration(cliOps, props);

    when(mdsApiClientBuilder.build()).thenReturn(mdsApiClient);

    AccessControlProviderFactory factory =
        new AccessControlProviderFactory(config, adminClient, mdsApiClientBuilder);

    AccessControlProvider provider = factory.get();

    verify(mdsApiClient, times(1)).setBasicAuth(new BasicAuth("alice", "alice-secret"));
    verify(mdsApiClient, times(1)).authenticate();

    assertThat(provider).isInstanceOf(RBACProvider.class);
  }

  @Test
  public void testACLsConfig() throws IOException {

    Configuration config = new Configuration(cliOps, props);

    AccessControlProviderFactory factory =
        new AccessControlProviderFactory(config, adminClient, mdsApiClientBuilder);

    assertThat(factory.get()).isInstanceOf(SimpleAclsProvider.class);
  }

  @Test
  public void testWrongProviderConfig() throws IOException {

    props.put(
        ACCESS_CONTROL_IMPLEMENTATION_CLASS, "com.purbon.kafka.topology.roles.MyCustomProvider");

    Configuration config = new Configuration(cliOps, props);

    AccessControlProviderFactory factory =
        new AccessControlProviderFactory(config, adminClient, mdsApiClientBuilder);
    assertThrows(IOException.class, factory::get);
  }
}
