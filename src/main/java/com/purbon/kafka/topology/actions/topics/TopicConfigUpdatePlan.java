package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.model.Topic;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
@Log4j2
@RequiredArgsConstructor
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
              String.format(
                  "addNewOrUpdatedConfigs compare: currentConfigEntryValue = %s and configValue = %s",
                  currentConfigEntry.value(), configValue));
          if (!currentConfigEntry.value().equals(configValue)) {
            log.debug(
                String.format(
                    "addNewOrUpdatedConfigs detected as different: currentConfigEntryValue = %s and configValue = %s",
                    currentConfigEntry.value(), configValue));
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
