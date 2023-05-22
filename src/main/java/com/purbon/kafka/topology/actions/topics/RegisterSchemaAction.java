package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.actions.BaseAction;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.schema.Subject;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RegisterSchemaAction extends BaseAction {

  private final Topic topic;
  private final String fullTopicName;
  private final List<SchemaChange> schemaChanges;

  public RegisterSchemaAction(
      SchemaRegistryManager schemaRegistryManager, Topic topic, String fullTopicName) {
    this.topic = topic;
    this.fullTopicName = fullTopicName;
    this.schemaChanges = findSchemaChanges(schemaRegistryManager, topic);
  }

  public boolean hasChanges() {
    return !schemaChanges.isEmpty();
  }

  private List<SchemaChange> findSchemaChanges(
      SchemaRegistryManager schemaRegistryManager, Topic topic) {
    return topic.getSchemas().stream()
        .flatMap(schema -> Stream.of(schema.getKeySubject(), schema.getValueSubject()))
        .flatMap(
            subject ->
                Stream.ofNullable(parseSchema(schemaRegistryManager, subject))
                    .map(
                        parsedSchema ->
                            new SchemaChange(
                                schemaRegistryManager,
                                subject,
                                subject.buildSubjectName(topic),
                                parsedSchema)))
        .filter(SchemaChange::hasChanges)
        .collect(Collectors.toList());
  }

  private ParsedSchema parseSchema(SchemaRegistryManager schemaRegistryManager, Subject subject) {
    return subject
        .getOptionalSchemaFile()
        .map(schemaRegistryManager::schemaFilePath)
        .map(schemaPath -> schemaRegistryManager.readSchemaFile(subject.getFormat(), schemaPath))
        .orElse(null);
  }

  @Override
  public void run() {
    log.debug("Register schemas for topic {}", fullTopicName);
    schemaChanges.forEach(SchemaChange::applyChanges);
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new LinkedHashMap<>();
    Map<String, ?> schemas =
        schemaChanges.stream()
            .sorted(SchemaChange.comparator)
            .collect(toSortedMap(SchemaChange::getSubjectName, SchemaChange::toProps));

    if (!schemas.isEmpty()) {
      map.put("Operation", getClass().getName());
      map.put("Topic", fullTopicName);
      map.put("Schemas", schemas);
    }
    return map;
  }

  @Override
  protected Collection<Map<String, Object>> detailedProps() {
    return topic.getSchemas().stream()
        .map(
            topicSchemas -> {
              String keySubjectName = topicSchemas.getKeySubject().buildSubjectName(topic);
              String valueSubjectName = topicSchemas.getValueSubject().buildSubjectName(topic);

              Map<String, ?> schemas =
                  Stream.concat(
                          schemaChanges.stream()
                              .filter(
                                  schemaChange ->
                                      schemaChange
                                          .getSubjectName()
                                          .equalsIgnoreCase(keySubjectName)),
                          schemaChanges.stream()
                              .filter(
                                  schemaChange ->
                                      schemaChange
                                          .getSubjectName()
                                          .equalsIgnoreCase(valueSubjectName)))
                      .collect(toSortedMap(SchemaChange::getSubjectName, SchemaChange::toProps));

              Map<String, Object> map = new LinkedHashMap<>();
              map.put(
                  "resource_name",
                  String.format(
                      "rn://register.schema/%s/%s/%s/%s",
                      getClass().getName(), fullTopicName, keySubjectName, valueSubjectName));
              map.put("operation", getClass().getName());
              map.put("topic", fullTopicName);
              map.put("schema", schemas);
              return map;
            })
        .collect(Collectors.toList());
  }
}
