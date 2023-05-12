package com.purbon.kafka.topology.actions;

import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseAccessControlAction extends BaseAction {

  protected Collection<TopologyAclBinding> aclBindings;

  public BaseAccessControlAction(Collection<TopologyAclBinding> aclBindings) {
    this.aclBindings = aclBindings;
  }

  protected BaseAccessControlAction() {
    this.aclBindings = new ArrayList<>();
  }

  @Override
  public void run() throws IOException {
    log.debug("Running Action {}", getClass());
    execute();
    if (!getAclBindings().isEmpty()) logResults();
  }

  private void logResults() {
    List<String> bindingsAsList =
        getAclBindings().stream()
            .filter(Objects::nonNull)
            .map(TopologyAclBinding::toString)
            .collect(Collectors.toList());
    log.debug("Bindings created {}", bindingsAsList);
  }

  protected abstract void execute() throws IOException;

  public List<TopologyAclBinding> getAclBindings() {
    return new ArrayList<>(aclBindings);
  }

  @Override
  protected List<Map<String, Object>> detailedProps() {
    return aclBindings.stream()
        .map(
            new Function<TopologyAclBinding, Map<String, Object>>() {
              @Override
              public Map<String, Object> apply(TopologyAclBinding binding) {
                Map<String, Object> map = new HashMap<>();
                map.put("resource_name", resourceNameBuilder(binding));
                map.put("operation", getClass().getName());
                map.put("acl.resource_type", binding.getResourceType());
                map.put("acl.resource_name", binding.getResourceName());
                map.put("acl.principal", binding.getPrincipal());
                map.put("acl.operation", binding.getOperation());
                map.put("acl.pattern", binding.getPattern());
                return map;
              }
            })
        .collect(Collectors.toList());
  }

  protected abstract String resourceNameBuilder(TopologyAclBinding binding);
}
