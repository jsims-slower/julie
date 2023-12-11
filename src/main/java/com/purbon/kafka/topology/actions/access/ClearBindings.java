package com.purbon.kafka.topology.actions.access;

import com.purbon.kafka.topology.AccessControlProvider;
import com.purbon.kafka.topology.actions.BaseAccessControlAction;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClearBindings extends BaseAccessControlAction {

  private final AccessControlProvider controlProvider;

  public ClearBindings(
      AccessControlProvider controlProvider, Collection<TopologyAclBinding> bindingsForRemoval) {
    this.aclBindings.addAll(bindingsForRemoval);
    this.controlProvider = controlProvider;
  }

  @Override
  protected void execute() throws IOException {
    log.debug("ClearBindings: {}", aclBindings);
    controlProvider.clearBindings(new HashSet<>(aclBindings));
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new HashMap<>();
    map.put("Operation", getClass().getName());
    map.put("Bindings", aclBindings);
    return map;
  }

  @Override
  protected String resourceNameBuilder(TopologyAclBinding binding) {
    return String.format(
        "rn://delete.binding/%s/%s/%s/%s/%s/%s",
        getClass().getName(),
        binding.getResourceType(),
        binding.getResourceName(),
        binding.getPrincipal(),
        binding.getOperation(),
        binding.getPattern());
  }
}
