package com.purbon.kafka.topology.actions;

import static com.purbon.kafka.topology.utils.Utils.filePath;

import com.purbon.kafka.topology.clients.ArtefactClient;
import com.purbon.kafka.topology.model.Artefact;
import com.purbon.kafka.topology.utils.Utils;
import java.io.IOException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SyncArtefactAction extends BaseAction {

  private final ArtefactClient client;
  private final Artefact artefact;
  private final String rootPath;

  public SyncArtefactAction(ArtefactClient client, String rootPath, Artefact artefact) {
    this.client = client;
    this.artefact = artefact;
    this.rootPath = rootPath;
  }

  @Override
  public void run() throws IOException {
    log.info("Updating artefact {} for client {}", artefact.getName(), client.getClass());
    client.update(artefact.getName(), content());
  }

  public Artefact getArtefact() {
    return artefact;
  }

  private String content() throws IOException {
    log.debug("Reading artefact content from {} with rootPath {}", artefact.getPath(), rootPath);
    return Utils.readFullFile(filePath(artefact.getPath(), rootPath));
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new HashMap<>();
    map.put("Operation", getClass().getName());
    map.put("Artefact", artefact.getPath());
    return map;
  }

  @Override
  protected List<Map<String, Object>> detailedProps() {
    Map<String, Object> map = new HashMap<>();
    map.put(
        "resource_name",
        String.format("rn://sync.artefact/%s/%s", getClass().getName(), artefact.getName()));
    map.put("operation", getClass().getName());
    map.put("artefact", artefact.getPath());
    return Collections.singletonList(map);
  }
}
