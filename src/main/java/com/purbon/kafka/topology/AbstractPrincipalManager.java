package com.purbon.kafka.topology;

import com.purbon.kafka.topology.model.Platform;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.User;
import com.purbon.kafka.topology.model.cluster.ServiceAccount;
import com.purbon.kafka.topology.serviceAccounts.VoidPrincipalProvider;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class AbstractPrincipalManager implements ExecutionPlanUpdater {

  private final List<String> managedPrefixes;
  protected PrincipalProvider provider;
  protected Configuration config;

  public AbstractPrincipalManager(PrincipalProvider provider, Configuration config) {
    this.provider = provider;
    this.config = config;
    this.managedPrefixes = config.getServiceAccountManagedPrefixes();
  }

  @Override
  public final void updatePlan(ExecutionPlan plan, Map<String, Topology> topologies)
      throws IOException {
    if (!config.enabledPrincipalManagement()) {
      log.debug(
          "Not running the PrincipalsManager as this is feature is not enabled by default, please enable if required.");
      return;
    }
    if (provider instanceof VoidPrincipalProvider) {
      // Do Nothing if the provider is the void one.
      // This means the management of principals is either not possible or has not been configured
      return;
    }
    provider.configure();

    Map<String, ServiceAccount> accounts = loadActualClusterStateIfAvailable(plan);
    for (Topology topology : topologies.values()) {
      List<String> principals = parseListOfPrincipals(topology);
      doUpdatePlan(plan, topology, principals, accounts);
    }
  }

  protected abstract void doUpdatePlan(
      ExecutionPlan plan,
      Topology topology,
      final List<String> principals,
      final Map<String, ServiceAccount> accounts);

  private Map<String, ServiceAccount> loadActualClusterStateIfAvailable(ExecutionPlan plan)
      throws IOException {
    Set<ServiceAccount> accounts =
        config.fetchStateFromTheCluster()
            ? provider.listServiceAccounts()
            : plan.getServiceAccounts();
    return accounts.stream()
        .filter(serviceAccount -> matchesPrefixList(serviceAccount.getName()))
        .collect(Collectors.toMap(ServiceAccount::getName, serviceAccount -> serviceAccount));
  }

  private boolean matchesPrefixList(String principal) {
    boolean matches =
        managedPrefixes.isEmpty() || managedPrefixes.stream().anyMatch(principal::startsWith);
    log.debug("Principal {} matches {} with {}", principal, matches, managedPrefixes);
    return matches;
  }

  private List<String> parseListOfPrincipals(Topology topology) {
    Stream<User> projectPrincipals =
        topology.getProjects().stream()
            .flatMap(
                project -> {
                  List<User> users = new ArrayList<>();
                  users.addAll(project.getConsumers());
                  users.addAll(project.getProducers());
                  users.addAll(project.getStreams());
                  users.addAll(project.getConnectors());
                  users.addAll(project.getSchemas());
                  for (Topic topic : project.getTopics()) {
                    users.addAll(topic.getConsumers());
                    users.addAll(topic.getProducers());
                  }
                  return users.stream();
                });

    List<User> platformPrincipals = new ArrayList<>();
    Platform platform = topology.getPlatform();
    platformPrincipals.addAll(platform.getControlCenter().getInstances());
    platformPrincipals.addAll(platform.getSchemaRegistry().getInstances());

    return Stream.concat(projectPrincipals, platformPrincipals.stream())
        .map(User::getPrincipal)
        .filter(this::matchesPrefixList)
        .collect(Collectors.toList());
  }

  @Override
  public final void printCurrentState(PrintStream out) throws IOException {
    out.println("List of Principles: ");
    provider.listServiceAccounts().forEach(out::println);
  }
}
