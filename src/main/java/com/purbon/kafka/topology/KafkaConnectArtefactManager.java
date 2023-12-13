package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.utils.Utils.filePath;

import com.fasterxml.jackson.databind.JsonNode;
import com.purbon.kafka.topology.api.connect.KConnectApiClient;
import com.purbon.kafka.topology.clients.ArtefactClient;
import com.purbon.kafka.topology.model.Artefact;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.artefact.KafkaConnectArtefact;
import com.purbon.kafka.topology.utils.JSON;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaConnectArtefactManager extends ArtefactManager {

  public KafkaConnectArtefactManager(
      ArtefactClient client, Configuration config, String topologyFileOrDir) {
    super(client, config, topologyFileOrDir);
  }

  @Override
  protected Collection<? extends Artefact> getLocalState(ExecutionPlan plan) {
    return plan.getConnectors();
  }

  public KafkaConnectArtefactManager(
      Map<String, KConnectApiClient> clients, Configuration config, String topologyFileOrDir) {
    super(clients, config, topologyFileOrDir);
  }

  @Override
  protected Collection<? extends Artefact> getClustersState() throws IOException {
    var kafkaConnectArtifacts = new HashSet<KafkaConnectArtefact>();
    for (var client : clients.values()) {
      client
          .getClusterState()
          // TODO: Are _all_ Artefacts KafkaConnectArtefacts?
          //  Unlike KSqlArtefactManager.getClustersState
          .forEach(
              artefact ->
                  kafkaConnectArtifacts.add(
                      new KafkaConnectArtefact(
                          artefact.getPath(),
                          reverseLookup(artefact.getServerLabel()),
                          artefact.getName(),
                          artefact.getHash())));
    }
    return kafkaConnectArtifacts;
  }

  private String reverseLookup(String host) {
    return config.getKafkaConnectServers().entrySet().stream()
        .filter(e -> host.equals(e.getValue()))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Failed to reverseLookup " + host));
  }

  @Override
  Set<KafkaConnectArtefact> parseNewArtefacts(Topology topology) {
    return topology.getProjects().stream()
        .flatMap(project -> project.getConnectorArtefacts().getConnectors().stream())
        .map(
            artefact -> {
              try {
                String config = Files.readString(filePath(artefact.getPath(), rootPath()));
                JsonNode configNode = JSON.toNode(config);
                String hash = Integer.toHexString(configNode.hashCode());
                return new KafkaConnectArtefact(
                    artefact.getPath(), artefact.getServerLabel(), artefact.getName(), hash);
              } catch (IOException e) {
                log.warn("Failed to compute hash for artefact " + artefact.getName() + ".", e);
                return artefact;
              }
            })
        .collect(Collectors.toSet());
  }

  @Override
  boolean isAllowDelete() {
    return config.isAllowDeleteConnectArtefacts();
  }

  @Override
  String rootPath() {
    return Files.isDirectory(Paths.get(topologyFileOrDir))
        ? topologyFileOrDir
        : new File(topologyFileOrDir).getParent();
  }

  @Override
  public void printCurrentState(PrintStream out) throws IOException {
    out.println("List of Connectors:");
    getClustersState().forEach(out::println);
  }
}
