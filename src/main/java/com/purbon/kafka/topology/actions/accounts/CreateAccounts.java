package com.purbon.kafka.topology.actions.accounts;

import com.purbon.kafka.topology.PrincipalProvider;
import com.purbon.kafka.topology.actions.BaseAccountsAction;
import com.purbon.kafka.topology.model.cluster.ServiceAccount;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateAccounts extends BaseAccountsAction {

  public CreateAccounts(PrincipalProvider provider, Set<ServiceAccount> accounts) {
    super(provider, accounts);
  }

  @Override
  public void run() throws IOException {
    log.debug("CreatePrincipals {}", principals);
    Set<ServiceAccount> mappedAccounts = new HashSet<>();
    for (ServiceAccount account : principals) {
      ServiceAccount sa =
          provider.createServiceAccount(account.getName(), account.getDescription());
      mappedAccounts.add(sa);
    }
    principals.clear();
    principals.addAll(mappedAccounts);
  }

  @Override
  protected String resourceNameBuilder(ServiceAccount account) {
    return String.format("rn://create.account/%s/%s", getClass().getName(), account.getName());
  }
}
