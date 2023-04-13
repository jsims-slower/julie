package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.actions.BaseAction;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.Topic;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.util.*;

@Log4j2
@RequiredArgsConstructor
public class CreateTopicAction extends BaseAction {

  private final TopologyBuilderAdminClient adminClient;
  private final Topic topic;
  private final String fullTopicName;

  public String getTopic() {
    return fullTopicName;
  }

  @Override
  public void run() throws IOException {
    createTopic(topic, fullTopicName);
  }

  private void createTopic(Topic topic, String fullTopicName) throws IOException {
    log.debug(String.format("Create new topic with name %s", fullTopicName));
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
