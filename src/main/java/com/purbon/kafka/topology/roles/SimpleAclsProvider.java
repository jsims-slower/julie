package com.purbon.kafka.topology.roles;

import com.purbon.kafka.topology.AccessControlProvider;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.acl.AclBinding;

@Slf4j
@RequiredArgsConstructor
public class SimpleAclsProvider implements AccessControlProvider {

  protected final TopologyBuilderAdminClient adminClient;

  @Override
  public void createBindings(Collection<TopologyAclBinding> bindings) throws IOException {
    log.debug("AclsProvider: createBindings");
    Collection<AclBinding> bindingsAsNativeKafka =
        bindings.stream()
            .map(TopologyAclBinding::asAclBinding)
            .flatMap(Optional::stream)
            .collect(Collectors.toList());
    log.debug("bindingsAsNativeKafka.size: {}", bindingsAsNativeKafka.size());
    adminClient.createAcls(bindingsAsNativeKafka);
  }

  @Override
  public void clearBindings(Collection<TopologyAclBinding> bindings) throws IOException {
    log.debug("AclsProvider: clearAcls");
    for (TopologyAclBinding binding : bindings) {
      try {
        adminClient.clearAcls(binding);
      } catch (IOException ex) {
        log.error(ex.getMessage(), ex);
        throw ex;
      }
    }
  }

  @Override
  public Map<String, List<TopologyAclBinding>> listAcls() {
    return adminClient.fetchAclsList().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry.getValue().stream()
                        .map(TopologyAclBinding::new)
                        .collect(Collectors.toList())));
  }
}
