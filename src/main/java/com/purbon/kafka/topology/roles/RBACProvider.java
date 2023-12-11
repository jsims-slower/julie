package com.purbon.kafka.topology.roles;

import com.purbon.kafka.topology.AccessControlProvider;
import com.purbon.kafka.topology.api.mds.MDSApiClient;
import com.purbon.kafka.topology.api.mds.RbacResourceType;
import com.purbon.kafka.topology.api.mds.RequestScope;
import java.io.IOException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class RBACProvider implements AccessControlProvider {

  private final MDSApiClient apiClient;

  public RBACProvider(MDSApiClient apiClient) {
    this.apiClient = apiClient;
  }

  @Override
  public void createBindings(Collection<TopologyAclBinding> bindings) throws IOException {
    log.debug("RBACProvider: createBindings");
    for (TopologyAclBinding binding : bindings) {
      apiClient.bindRequest(binding);
    }
  }

  @Override
  public void clearBindings(Collection<TopologyAclBinding> bindings) {
    log.debug("RBACProvider: clearAcls");
    bindings.forEach(
        aclBinding -> {
          String principal = aclBinding.getPrincipal();
          String role = aclBinding.getOperation();

          RequestScope scope = new RequestScope();

          String resourceType = StringUtils.capitalize(aclBinding.getResourceType().toLowerCase());

          var clusterIds = apiClient.withClusterIDs().forKafka();

          if (resourceType.equalsIgnoreCase("subject")) {
            clusterIds = clusterIds.forSchemaRegistry();
          } else if (resourceType.equalsIgnoreCase("connector")) {
            clusterIds = clusterIds.forKafkaConnect();
          } else if (resourceType.equalsIgnoreCase("KsqlCluster")) {
            clusterIds = clusterIds.forKsql();
          }

          scope.setClusters(clusterIds.asMap());
          scope.addResource(resourceType, aclBinding.getResourceName(), aclBinding.getPattern());
          scope.build();

          apiClient.deleteRole(principal, role, scope);
        });
  }

  @Override
  public Map<String, List<TopologyAclBinding>> listAcls() {
    Map<String, List<TopologyAclBinding>> map = new HashMap<>();
    for (String roleName : apiClient.getRoleNames()) {
      for (String principalName : apiClient.lookupKafkaPrincipalsByRoleForKafka(roleName)) {
        for (RbacResourceType resource :
            apiClient.lookupResourcesForKafka(principalName, roleName)) {
          if (!map.containsKey(resource.getName())) {
            map.put(resource.getName(), new ArrayList<>());
          }
          TopologyAclBinding binding =
              TopologyAclBinding.build(
                  normalize(resource.getResourceType()),
                  resource.getName(),
                  "*",
                  roleName,
                  principalName,
                  resource.getPatternType());
          map.get(resource.getName()).add(binding);
        }
      }
    }
    return map;
  }

  private String normalize(String resourceType) {
    String[] fields = resourceType.split("(?=\\p{Upper})");
    return String.join("_", fields).toUpperCase();
  }
}
