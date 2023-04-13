package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.actions.BaseAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.Topic;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Log4j2
@RequiredArgsConstructor
public class UpdateTopicConfigAction extends BaseAction {

  private final TopologyBuilderAdminClient adminClient;
  private final TopicConfigUpdatePlan topicConfigUpdatePlan;

  @Override
  public void run() throws IOException {
    final Topic topic = topicConfigUpdatePlan.getTopic();
    final String fullTopicName = topicConfigUpdatePlan.getFullTopicName();

    log.debug(String.format("Update config for topic %s", fullTopicName));
    if (topicConfigUpdatePlan.isUpdatePartitionCount()) {
      log.debug(String.format("Update partition count of topic %s", fullTopicName));
      adminClient.updatePartitionCount(topic, fullTopicName);
    }

    adminClient.updateTopicConfig(topicConfigUpdatePlan);
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> changes = new LinkedHashMap<>();
    if (topicConfigUpdatePlan.hasNewConfigs()) {
      changes.put("NewConfigs", sortMap(topicConfigUpdatePlan.getNewConfigValues()));
    }
    if (topicConfigUpdatePlan.hasUpdatedConfigs()) {
      changes.put("UpdatedConfigs", formatUpdated(topicConfigUpdatePlan.getUpdatedConfigValues()));
    }
    if (topicConfigUpdatePlan.hasDeletedConfigs()) {
      changes.put("DeletedConfigs", sortMap(topicConfigUpdatePlan.getDeletedConfigValues()));
    }
    if (topicConfigUpdatePlan.isUpdatePartitionCount()) {
      changes.put("UpdatedPartitionCount", topicConfigUpdatePlan.getTopicPartitionCount());
    }

    Map<String, Object> map = new LinkedHashMap<>();
    map.put("Operation", getClass().getName());
    map.put("Topic", topicConfigUpdatePlan.getFullTopicName());
    map.put("Action", "update");
    map.put("Changes", changes);
    return map;
  }

  @Override
  protected List<Map<String, Object>> detailedProps() {
    Map<String, Object> map = new HashMap<>();
    props().forEach((key, value) -> map.put(key.toLowerCase(Locale.ROOT), value));
    map.remove("action");
    map.put(
        "resource_name",
        String.format(
            "rn://update.topic.config/%s/%s",
            getClass().getName(), topicConfigUpdatePlan.getTopic().getName()));
    return Collections.singletonList(map);
  }

  private <T> Map<String, ?> formatUpdated(Map<String, Pair<T, T>> updatedMap) {
    return updatedMap
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> String.format("%s (%s)", entry.getValue().getRight(), entry.getValue().getLeft()),
            //entry -> sortMap(Map.of(
            //    "new", entry.getValue().getRight(),
            //    "old", entry.getValue().getLeft()
            //)),
            (v1, v2) -> {
              throw new RuntimeException(String.format(
                  "Collectors.toMap had a conflict [%s] vs [%s]",
                  v1, v2));
            },
            TreeMap::new
        ));
  }
}
