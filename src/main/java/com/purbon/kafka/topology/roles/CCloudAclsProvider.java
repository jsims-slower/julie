package com.purbon.kafka.topology.roles;

import com.purbon.kafka.topology.AccessControlProvider;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.api.ccloud.CCloudApi;
import com.purbon.kafka.topology.utils.CCloudUtils;
import java.io.IOException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CCloudAclsProvider extends SimpleAclsProvider implements AccessControlProvider {

  private final CCloudApi cli;
  private final String clusterId;
  private final Configuration config;
  private final CCloudUtils cCloudUtils;

  public CCloudAclsProvider(
      final TopologyBuilderAdminClient adminClient, final Configuration config) throws IOException {
    super(adminClient);
    this.cli = new CCloudApi(config.getConfluentCloudClusterUrl(), config);
    this.clusterId = config.getConfluentCloudClusterId();
    this.config = config;
    this.cCloudUtils = new CCloudUtils(config);
  }

  @Override
  public void createBindings(Collection<TopologyAclBinding> bindings) throws IOException {
    var serviceAccountIdByNameMap = cCloudUtils.initializeLookupTable(this.cli);
    for (TopologyAclBinding binding : bindings) {
      cli.createAcl(
          clusterId, cCloudUtils.translateIfNecessary(binding, serviceAccountIdByNameMap));
    }
  }

  @Override
  public void clearBindings(Collection<TopologyAclBinding> bindings) throws IOException {
    var serviceAccountIdByNameMap = cCloudUtils.initializeLookupTable(this.cli);
    for (TopologyAclBinding binding : bindings) {
      cli.deleteAcls(
          clusterId, cCloudUtils.translateIfNecessary(binding, serviceAccountIdByNameMap));
    }
  }

  @Override
  public Map<String, List<TopologyAclBinding>> listAcls() {
    try {
      Map<String, List<TopologyAclBinding>> bindings = new HashMap<>();
      for (TopologyAclBinding binding : cli.listAcls(clusterId)) {
        String resourceName = binding.getResourceName();
        if (!bindings.containsKey(resourceName)) {
          bindings.put(resourceName, new ArrayList<>());
        }
        bindings.get(resourceName).add(binding);
      }
      return bindings;
    } catch (IOException e) {
      log.warn(e.getMessage(), e);
      return Collections.emptyMap();
    }
  }
}
