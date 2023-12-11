package com.purbon.kafka.topology.actions;

import com.purbon.kafka.topology.PrincipalProvider;
import com.purbon.kafka.topology.model.cluster.ServiceAccount;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class BaseAccountsAction extends BaseAction {

  protected final PrincipalProvider provider;
  @Getter protected final Collection<ServiceAccount> principals;

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new HashMap<>();
    map.put("Operation", getClass().getName());
    map.put("Principals", principals);
    return map;
  }

  @Override
  protected List<Map<String, Object>> detailedProps() {
    return principals.stream()
        .map(
            account -> {
              Map<String, Object> map = new HashMap<>();
              map.put("resource_name", resourceNameBuilder(account));
              map.put("operation", getClass().getName());
              map.put("principal", account.getName());
              return map;
            })
        .collect(Collectors.toList());
  }

  protected abstract String resourceNameBuilder(ServiceAccount account);

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BaseAccountsAction)) {
      return false;
    }
    BaseAccountsAction that = (BaseAccountsAction) o;
    return Objects.equals(provider, that.provider) && Objects.equals(principals, that.principals);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, principals);
  }
}
