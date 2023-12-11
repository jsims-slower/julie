package com.purbon.kafka.topology.actions.accounts;

import com.purbon.kafka.topology.PrincipalProvider;
import com.purbon.kafka.topology.actions.BaseAccountsAction;
import com.purbon.kafka.topology.model.cluster.ServiceAccount;
import java.io.IOException;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClearAccounts extends BaseAccountsAction {

  public ClearAccounts(PrincipalProvider provider, Collection<ServiceAccount> accounts) {
    super(provider, accounts);
  }

  @Override
  public void run() throws IOException {
    log.debug("ClearPrincipals {}", principals);
    for (ServiceAccount account : principals) {
      provider.deleteServiceAccount(account);
    }
  }

  @Override
  protected String resourceNameBuilder(ServiceAccount account) {
    return String.format("rn://delete.account/%s/%s", getClass().getName(), account.getName());
  }
}
