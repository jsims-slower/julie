package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.model.Topic;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

@Data
@Slf4j
public class TopicConfigUpdatePlan {

  private final Topic topic;
  private boolean updatePartitionCount;
  private final Map<String, String> newConfigValues = new HashMap<>();
  private final Map<String, Pair<String, String>> updatedConfigValues = new HashMap<>();
  private final Map<String, String> deletedConfigValues = new HashMap<>();

  public void addNewConfig(String name, String value) {
    newConfigValues.put(name, value);
  }

  public void addConfigToUpdate(String name, String currentValue, String newValue) {
    updatedConfigValues.put(name, Pair.of(currentValue, newValue));
  }

  public void addConfigToDelete(String name, String value) {
    deletedConfigValues.put(name, value);
  }

  public String getFullTopicName() {
    return topic.toString();
  }

  public boolean hasNewConfigs() {
    return !newConfigValues.isEmpty();
  }

  public boolean hasUpdatedConfigs() {
    return !updatedConfigValues.isEmpty();
  }

  public boolean hasDeletedConfigs() {
    return !deletedConfigValues.isEmpty();
  }

  public Integer getTopicPartitionCount() {
    return topic.partitionsCount();
  }

  public boolean hasConfigChanges() {
    return updatePartitionCount || hasNewConfigs() || hasUpdatedConfigs() || hasDeletedConfigs();
  }

  public void addNewOrUpdatedConfigs(
      HashMap<String, String> topicConfigs, Config currentKafkaConfigs) {
    topicConfigs.forEach(
        (configKey, configValue) -> {
          ConfigEntry currentConfigEntry = currentKafkaConfigs.get(configKey);
          log.debug(
              "addNewOrUpdatedConfigs compare: currentConfigEntryValue = {} and configValue = {}",
              currentConfigEntry.value(),
              configValue);
          if (!currentConfigEntry.value().equals(configValue)) {
            log.debug(
                "addNewOrUpdatedConfigs detected as different: currentConfigEntryValue = {} and configValue = {}",
                currentConfigEntry.value(),
                configValue);
            if (isDynamicTopicConfig(currentConfigEntry)) {
              addConfigToUpdate(configKey, currentConfigEntry.value(), configValue);
            } else {
              addNewConfig(configKey, configValue);
            }
          }
        });
  }

  public void addDeletedConfigs(HashMap<String, String> topicConfigs, Config currentKafkaConfigs) {
    Set<String> configKeys = topicConfigs.keySet();
    currentKafkaConfigs
        .entries()
        .forEach(
            entry -> {
              if (isDynamicTopicConfig(entry) && !configKeys.contains(entry.name())) {
                addConfigToDelete(entry.name(), entry.value());
              }
            });
  }

  private boolean isDynamicTopicConfig(ConfigEntry currentConfigEntry) {
    return currentConfigEntry.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
  }
}
