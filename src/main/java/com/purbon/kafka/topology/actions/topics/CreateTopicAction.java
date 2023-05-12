package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.actions.BaseAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.Topic;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateTopicAction extends BaseAction {

  private final TopologyBuilderAdminClient adminClient;
  private final Topic topic;
  @Getter private final String fullTopicName;

  @Override
  public void run() throws IOException {
    createTopic(topic, fullTopicName);
  }

  private void createTopic(Topic topic, String fullTopicName) throws IOException {
    log.debug("Create new topic with name {}", fullTopicName);
    adminClient.createTopic(topic, fullTopicName);
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("Operation", getClass().getName());
    map.put("Topic", fullTopicName);
    map.put("Action", "create");
    map.put("Configs", sortMap(topic.getConfig()));
    return map;
  }

  @Override
  protected List<Map<String, Object>> detailedProps() {
    Map<String, Object> map = new HashMap<>();
    map.put(
        "resource_name",
        String.format("rn://create.topic/%s/%s", getClass().getName(), fullTopicName));
    map.put("operation", getClass().getName());
    map.put("topic", fullTopicName);
    map.put("config", topic.getConfig());
    return Collections.singletonList(map);
  }
}
