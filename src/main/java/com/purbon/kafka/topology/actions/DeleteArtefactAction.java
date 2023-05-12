package com.purbon.kafka.topology.actions;

import com.purbon.kafka.topology.clients.ArtefactClient;
import com.purbon.kafka.topology.model.Artefact;
import com.purbon.kafka.topology.model.artefact.TypeArtefact;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeleteArtefactAction extends BaseAction {

  private final ArtefactClient client;
  private final Artefact artefact;

  @Override
  public void run() throws IOException {
    log.debug("Deleting artefact {} with client {}", artefact.getName(), client.getClass());

    if (artefact.getClass().isAnnotationPresent(TypeArtefact.class)) {
      TypeArtefact annon = artefact.getClass().getAnnotation(TypeArtefact.class);
      log.debug("Deleting artefact with type {}", annon.name());
      client.delete(artefact.getName(), annon.name());
    } else {
      client.delete(artefact.getName());
    }
  }

  public Artefact getArtefact() {
    return artefact;
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
        String.format("rn://delete.artefact/%s/%s", getClass().getName(), artefact.getName()));
    map.put("operation", getClass().getName());
    map.put("artefact", artefact.getPath());
    return Collections.singletonList(map);
  }
}
