package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.roles.rbac.RBACBindingsBuilder.LITERAL;
import static com.purbon.kafka.topology.roles.rbac.RBACPredefinedRoles.DEVELOPER_READ;
import static com.purbon.kafka.topology.roles.rbac.RBACPredefinedRoles.RESOURCE_OWNER;
import static com.purbon.kafka.topology.roles.rbac.RBACPredefinedRoles.SECURITY_ADMIN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.purbon.kafka.topology.api.mds.AuthenticationCredentials;
import com.purbon.kafka.topology.api.mds.MDSApiClient;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import com.purbon.kafka.topology.utils.BasicAuth;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("Zookeeper issues")
public class MDSApiClientRbacIT extends MDSBaseTest {

  private final String mdsUser = "professor";
  private final String mdsPassword = "professor";

  private MDSApiClient apiClient;

  @BeforeEach
  public void before() throws IOException, InterruptedException {
    super.beforeEach();
    String mdsServer = "http://localhost:8090";
    apiClient = new MDSApiClient(mdsServer);
  }

  @Test
  public void testMDSLogin() throws IOException {
    apiClient.setBasicAuth(new BasicAuth(mdsUser, mdsPassword));
    apiClient.authenticate();
    AuthenticationCredentials credentials = apiClient.getCredentials();
    assertThat(credentials.getAuthToken()).isNotEmpty();
  }

  @Test
  public void testWithWrongMDSLogin() {
    apiClient.setBasicAuth(new BasicAuth("wrong-user", "wrong-password"));
    assertThrows(IOException.class, apiClient::authenticate);
  }

  @Test
  public void testLookupRoles() throws IOException {
    apiClient.setBasicAuth(new BasicAuth(mdsUser, mdsPassword));
    apiClient.authenticate();
    apiClient.setKafkaClusterId(getKafkaClusterID());

    List<String> roles = apiClient.lookupRoles("User:fry");
    assertThat(roles).contains(DEVELOPER_READ);
  }

  @Test
  public void testBindRoleToResource() throws IOException {
    apiClient.setBasicAuth(new BasicAuth(mdsUser, mdsPassword));
    apiClient.authenticate();
    apiClient.setKafkaClusterId(getKafkaClusterID());

    TopologyAclBinding binding =
        apiClient.bind("User:fry", DEVELOPER_READ, "connect-configs", LITERAL);

    apiClient.bindRequest(binding);

    List<String> roles = apiClient.lookupRoles("User:fry");
    assertThat(roles).containsOnly(DEVELOPER_READ);
  }

  @Test
  public void testBindRoleWithoutAuthentication() {
    apiClient.setKafkaClusterId(getKafkaClusterID());

    TopologyAclBinding binding =
        apiClient.bind("User:fry", DEVELOPER_READ, "connect-configs", LITERAL);

    assertThrows(IOException.class, () -> apiClient.bindRequest(binding));
  }

  @Test
  public void testBindSecurityAdminRole() throws IOException {
    apiClient.setBasicAuth(new BasicAuth(mdsUser, mdsPassword));
    apiClient.authenticate();
    apiClient.setKafkaClusterId(getKafkaClusterID());
    apiClient.setSchemaRegistryClusterID("schema-registry");
    String principal = "User:foo" + System.currentTimeMillis();

    TopologyAclBinding binding =
        apiClient.bind(principal, SECURITY_ADMIN).forSchemaRegistry().apply();

    apiClient.bindRequest(binding);

    Map<String, Map<String, String>> clusters =
        apiClient.withClusterIDs().forKafka().forSchemaRegistry().asMap();

    List<String> roles = apiClient.lookupRoles(principal, clusters);
    assertThat(roles).containsOnly(SECURITY_ADMIN);
  }

  @Test
  public void testBindSubjectRole() throws IOException {
    apiClient.setBasicAuth(new BasicAuth(mdsUser, mdsPassword));
    apiClient.authenticate();
    apiClient.setKafkaClusterId(getKafkaClusterID());
    apiClient.setSchemaRegistryClusterID("schema-registry");

    String principal = "User:foo" + System.currentTimeMillis();
    String subject = "topic-value";

    TopologyAclBinding binding =
        apiClient
            .bind(principal, DEVELOPER_READ)
            .forSchemaSubject(subject)
            .apply("Subject", subject);

    apiClient.bindRequest(binding);

    Map<String, Map<String, String>> clusters =
        apiClient.withClusterIDs().forKafka().forSchemaRegistry().asMap();

    List<String> roles = apiClient.lookupRoles(principal, clusters);
    assertThat(roles).containsOnly(DEVELOPER_READ);
  }

  @Test
  public void testBindResourceOwnerRole() throws IOException {
    apiClient.setBasicAuth(new BasicAuth(mdsUser, mdsPassword));
    apiClient.authenticate();
    apiClient.setKafkaClusterId(getKafkaClusterID());

    String principal = "User:fry" + System.currentTimeMillis();
    TopologyAclBinding binding =
        apiClient.bind(principal, RESOURCE_OWNER, "connect-configs", LITERAL);
    apiClient.bindRequest(binding);

    List<String> roles = apiClient.lookupRoles(principal);
    assertThat(roles).containsOnly(RESOURCE_OWNER);
  }
}
