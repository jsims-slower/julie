package com.purbon.kafka.topology;

import com.purbon.kafka.topology.backend.Backend;
import com.purbon.kafka.topology.backend.BackendState;
import com.purbon.kafka.topology.backend.FileBackend;
import com.purbon.kafka.topology.model.artefact.KafkaConnectArtefact;
import com.purbon.kafka.topology.model.artefact.KsqlStreamArtefact;
import com.purbon.kafka.topology.model.artefact.KsqlTableArtefact;
import com.purbon.kafka.topology.model.cluster.ServiceAccount;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackendController {

  public static final String STATE_FILE_NAME = ".cluster-state";

  public enum Mode {
    TRUNCATE,
    APPEND
  }

  @Getter private final Backend backend;
  @Getter private BackendState state;

  public BackendController() {
    this(new FileBackend());
  }

  public BackendController(Backend backend) {
    this.backend = backend;
    this.state = new BackendState();
  }

  public void addBindings(List<TopologyAclBinding> bindings) {
    log.debug("Adding bindings {} to the backend", bindings);
    state.addBindings(bindings);
  }

  public void addTopics(Set<String> topics) {
    log.debug("Adding topics {} to the backend", topics);
    state.addTopics(topics);
  }

  public void addServiceAccounts(Set<ServiceAccount> serviceAccounts) {
    log.debug("Adding Service Accounts {} to the backend", serviceAccounts);
    state.addAccounts(serviceAccounts);
  }

  public void addConnectors(Set<KafkaConnectArtefact> connectors) {
    log.debug("Adding Connectors {} to the backend", connectors);
    state.addConnectors(connectors);
  }

  public void addKSqlStreams(Set<KsqlStreamArtefact> ksqlStreams) {
    log.debug("Adding KSQL Streams {} to the backend", ksqlStreams);
    state.addKSqlStreams(ksqlStreams);
  }

  public void addKSqlTables(Set<KsqlTableArtefact> ksqlTable) {
    log.debug("Adding KSQL Table {} to the backend", ksqlTable);
    state.addKSqlTables(ksqlTable);
  }

  public Set<ServiceAccount> getServiceAccounts() {
    return state.getAccounts();
  }

  public Set<TopologyAclBinding> getBindings() {
    return state.getBindings();
  }

  public Set<String> getTopics() {
    return state.getTopics();
  }

  public Set<KafkaConnectArtefact> getConnectors() {
    return state.getConnectors();
  }

  public Set<KsqlStreamArtefact> getKSqlStreams() {
    return state.getKsqlStreams();
  }

  public Set<KsqlTableArtefact> getKSqlTables() {
    return state.getKsqlTables();
  }

  public void flushAndClose() throws IOException {
    log.debug("Flush data from the backend at {}", backend.getClass());
    backend.createOrOpen(Mode.TRUNCATE);
    backend.save(state);
    backend.close();
  }

  public void load() throws IOException {
    log.debug("Loading data from the backend at {}", backend.getClass());
    backend.createOrOpen();
    state = backend.load();
  }

  public void reset() {
    log.debug("Reset the bindings cache");
    state.clear();
  }

  public int size() {
    return state.size();
  }
}
