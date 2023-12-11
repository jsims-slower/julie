package com.purbon.kafka.topology.actions.access;

import com.purbon.kafka.topology.AccessControlProvider;
import com.purbon.kafka.topology.actions.BaseAccessControlAction;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateBindings extends BaseAccessControlAction {

  private final AccessControlProvider controlProvider;

  public CreateBindings(AccessControlProvider controlProvider, Set<TopologyAclBinding> bindings) {
    this.aclBindings.addAll(bindings);
    this.controlProvider = controlProvider;
  }

  @Override
  protected void execute() throws IOException {
    log.debug("CreateBindings: {}", aclBindings);
    controlProvider.createBindings(new HashSet<>(aclBindings));
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
        "rn://create.binding/%s/%s/%s/%s/%s/%s",
        getClass().getName(),
        binding.getResourceType(),
        binding.getResourceName(),
        binding.getPrincipal(),
        binding.getOperation(),
        binding.getPattern());
  }
}
