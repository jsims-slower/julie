package com.purbon.kafka.topology;

import com.purbon.kafka.topology.clients.ArtefactClient;
import com.purbon.kafka.topology.model.Artefact;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.artefact.KsqlArtefact;
import com.purbon.kafka.topology.model.artefact.KsqlArtefacts;
import com.purbon.kafka.topology.model.artefact.KsqlStreamArtefact;
import com.purbon.kafka.topology.model.artefact.KsqlTableArtefact;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KSqlArtefactManager extends ArtefactManager {

  public KSqlArtefactManager(
      ArtefactClient client, Configuration config, String topologyFileOrDir) {
    super(client, config, topologyFileOrDir);
  }

  public KSqlArtefactManager(
      Map<String, ? extends ArtefactClient> clients,
      Configuration config,
      String topologyFileOrDir) {
    super(clients, config, topologyFileOrDir);
  }

  @Override
  protected Collection<? extends Artefact> getLocalState(ExecutionPlan plan) {
    return plan.getKSqlArtefacts();
  }

  @Override
  protected List<? extends Artefact> findArtefactsToBeDeleted(
      Collection<? extends Artefact> currentArtefacts, Set<Artefact> artefacts) {

    var artefactsList =
        currentArtefacts.stream()
            .filter(a -> !artefacts.contains(a))
            .sorted((o1, o2) -> -1 * ((KsqlArtefact) o1).compareTo((KsqlArtefact) o2))
            .collect(Collectors.toCollection(LinkedList::new));

    Map<String, LinkedList<KsqlArtefact>> artefactsMap = new HashMap<>();
    artefactsMap.put("table", new LinkedList<>());
    artefactsMap.put("stream", new LinkedList<>());

    artefactsList.forEach(
        (Consumer<Artefact>)
            artefact -> {
              if (artefact instanceof KsqlTableArtefact) {
                artefactsMap.get("table").add((KsqlArtefact) artefact);
              } else {
                artefactsMap.get("stream").add((KsqlArtefact) artefact);
              }
            });

    LinkedList<KsqlArtefact> toDeleteArtefactsList = new LinkedList<>();
    for (String key : Arrays.asList("table", "stream")) {
      artefactsMap.get(key).descendingIterator().forEachRemaining(toDeleteArtefactsList::add);
    }
    return toDeleteArtefactsList;
  }

  @Override
  protected Collection<? extends Artefact> getClustersState() throws IOException {
    var ksqlArtefacts = new HashSet<KsqlArtefact>();
    for (var client : clients.values()) {
      client
          .getClusterState()
          .forEach(
              artefact -> {
                if (artefact instanceof KsqlStreamArtefact) {
                  ksqlArtefacts.add(
                      new KsqlStreamArtefact(artefact.getPath(), null, artefact.getName()));
                } else if (artefact instanceof KsqlTableArtefact) {
                  ksqlArtefacts.add(
                      new KsqlTableArtefact(artefact.getPath(), null, artefact.getName()));
                } else {
                  log.error("KSQL Artefact of wrong type " + artefact.getClass());
                }
              });
    }
    return ksqlArtefacts;
  }

  @Override
  Set<Artefact> parseNewArtefacts(Topology topology) {
    return topology.getProjects().stream()
        .flatMap(
            (Function<Project, Stream<Artefact>>)
                project -> {
                  KsqlArtefacts kSql = project.getKsqlArtefacts();
                  return Stream.concat(
                      Stream.concat(kSql.getStreams().stream(), kSql.getTables().stream()),
                      Stream.of(kSql.getVars()));
                })
        .sorted()
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  @Override
  boolean isAllowDelete() {
    return config.isAllowDeleteKsqlArtefacts();
  }

  @Override
  String rootPath() {
    return Files.isDirectory(Paths.get(topologyFileOrDir))
        ? topologyFileOrDir
        : new File(topologyFileOrDir).getParent();
  }

  @Override
  public void printCurrentState(PrintStream out) throws IOException {
    out.println("List of KSQL Artifacts:");
    getClustersState().forEach(out::println);
  }
}
