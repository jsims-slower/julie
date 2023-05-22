package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.actions.BaseAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeleteTopics extends BaseAction {

  private final TopologyBuilderAdminClient adminClient;
  @Getter private final List<String> topicsToBeDeleted;

  @Override
  public void run() throws IOException {
    log.debug("Delete topics: {}", topicsToBeDeleted);
    adminClient.deleteTopics(topicsToBeDeleted);
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("Operation", getClass().getName());
    map.put("topics", topicsToBeDeleted);
    return map;
  }

  @Override
  protected Collection<Map<String, Object>> detailedProps() {
    return topicsToBeDeleted.stream()
        .map(
            topic -> {
              Map<String, Object> map = new HashMap<>();
              map.put(
                  "resource_name",
                  String.format("rn://delete.topic/%s/%s", getClass().getName(), topic));
              map.put("operation", getClass().getName());
              map.put("topic", topic);
              return map;
            })
        .collect(Collectors.toList());
  }
}
