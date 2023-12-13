package com.purbon.kafka.topology.actions;

import static com.purbon.kafka.topology.utils.Utils.filePath;

import com.purbon.kafka.topology.clients.ArtefactClient;
import com.purbon.kafka.topology.model.Artefact;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SyncArtefactAction extends BaseAction {

  private final ArtefactClient client;
  private final String rootPath;
  @Getter private final Artefact artefact;

  @Override
  public void run() throws IOException {
    log.info("Updating artefact {} for client {}", artefact.getName(), client.getClass());
    client.update(artefact.getName(), content());
  }

  private String content() throws IOException {
    log.debug("Reading artefact content from {} with rootPath {}", artefact.getPath(), rootPath);
    return Files.readString(filePath(artefact.getPath(), rootPath));
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
