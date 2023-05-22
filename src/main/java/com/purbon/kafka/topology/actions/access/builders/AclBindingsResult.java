package com.purbon.kafka.topology.actions.access.builders;

import com.purbon.kafka.topology.roles.TopologyAclBinding;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;

@Getter
@RequiredArgsConstructor
public final class AclBindingsResult {

  private final Collection<TopologyAclBinding> aclBindings;
  private final String errorMessage;

  public static AclBindingsResult forError(String errorMessage) {
    return new AclBindingsResult(null, errorMessage);
  }

  public static AclBindingsResult forAclBindings(Collection<TopologyAclBinding> aclBindings) {
    return new AclBindingsResult(aclBindings, null);
  }

  public boolean isError() {
    return errorMessage != null;
  }
}
