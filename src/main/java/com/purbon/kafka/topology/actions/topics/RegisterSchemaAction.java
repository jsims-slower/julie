package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.actions.BaseAction;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.schema.Subject;
import com.purbon.kafka.topology.model.schema.TopicSchemas;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import io.confluent.kafka.schemaregistry.ParsedSchema;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class RegisterSchemaAction extends BaseAction {

  private final Topic topic;
  private final String fullTopicName;
  private final SchemaRegistryManager schemaRegistryManager;
  private final List<SchemaChange> schemaChanges;

  @RequiredArgsConstructor
  private static final class SchemaChange {
    public final Subject subject;
    public final String subjectName;
    public final Integer schemaId;
    public final ParsedSchema parsedSchema;
    public final String oldCompatibility;
  }

  public static RegisterSchemaAction createIfHasChanged(SchemaRegistryManager schemaRegistryManager,
                                                        Topic topic,
                                                        String fullTopicName) {
    List<SchemaChange> schemaChanges = new LinkedList<>();
    for (TopicSchemas schema : topic.getSchemas()) {
      Optional
          .ofNullable(checkSchemaChanged(schemaRegistryManager, topic, schema.getKeySubject()))
          .ifPresent(schemaChanges::add);
      Optional
          .ofNullable(checkSchemaChanged(schemaRegistryManager, topic, schema.getValueSubject()))
          .ifPresent(schemaChanges::add);
    }
    return schemaChanges.isEmpty()
        ? null
        : new RegisterSchemaAction(schemaRegistryManager, topic, fullTopicName, schemaChanges);
  }

  public RegisterSchemaAction(SchemaRegistryManager schemaRegistryManager,
                              Topic topic,
                              String fullTopicName,
                              List<SchemaChange> schemaChanges) {
    this.topic = topic;
    this.fullTopicName = fullTopicName;
    this.schemaRegistryManager = schemaRegistryManager;
    this.schemaChanges = schemaChanges;
  }

  public String getTopic() {
    return fullTopicName;
  }

  @Override
  public void run() {
    log.debug(String.format("Register schemas for topic %s", fullTopicName));

    for (SchemaChange schemaChange : schemaChanges) {
      registerSchema(schemaChange);
      setCompatibility(schemaChange);
    }
  }

  private void registerSchema(SchemaChange schemaChange) {
    schemaRegistryManager.register(schemaChange.subjectName, schemaChange.parsedSchema);
  }

  private void setCompatibility(SchemaChange schemaChange) {
    schemaChange
        .subject
        .getOptionalCompatibility()
        .ifPresent(compatibility ->
            schemaRegistryManager.setCompatibility(schemaChange.subjectName, compatibility)
        );
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new LinkedHashMap<>();
    Map<String, ?> schemas =
        schemaChanges
            .stream()
            .sorted(Comparator.comparing(schemaChange -> schemaChange.subjectName))
            .collect(Collectors.toMap(
                schemaChange -> schemaChange.subjectName,
                this::toProps,
                (v1, v2) -> {
                  throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));
                },
                TreeMap::new
            ));

    if (!schemas.isEmpty()) {
      map.put("Operation", getClass().getName());
      map.put("Topic", fullTopicName);
      map.put("Schemas", schemas);
    }
    return map;
  }

  @Override
  protected List<Map<String, Object>> detailedProps() {
    return topic.getSchemas().stream()
        .map(topicSchemas -> {
          String keySubjectName = topicSchemas.getKeySubject().buildSubjectName(topic);
          String valueSubjectName = topicSchemas.getValueSubject().buildSubjectName(topic);

          Map<String, ?> schemas = Stream
              .concat(
                  schemaChanges.stream().filter(schemaChange ->
                      schemaChange.subjectName.equalsIgnoreCase(keySubjectName)
                  ),
                  schemaChanges.stream().filter(schemaChange ->
                      schemaChange.subjectName.equalsIgnoreCase(valueSubjectName)
                  )
              )
              .collect(Collectors.toMap(
                  schemaChange -> schemaChange.subjectName,
                  this::toProps,
                  (v1, v2) -> {
                    throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));
                  },
                  TreeMap::new
              ));

          Map<String, Object> map = new LinkedHashMap<>();
          map.put(
              "resource_name",
              String.format(
                  "rn://register.schema/%s/%s/%s/%s",
                  getClass().getName(),
                  fullTopicName,
                  keySubjectName,
                  valueSubjectName
              )
          );
          map.put("operation", getClass().getName());
          map.put("topic", fullTopicName);
          map.put("schema", schemas);
          return map;
        })
        .collect(Collectors.toList());
  }

  private Map<String, String> toProps(SchemaChange schemaChange) {
    Map<String, String> subjectInfo = new LinkedHashMap<>();
    subjectInfo.put(
        String.format("[%s]file", schemaChange.schemaId == null ? "*" : ""),
        schemaChange.subject.getOptionalSchemaFile().orElse("")
    );
    subjectInfo.put("format", schemaChange.subject.getFormat());
    subjectInfo.put(
        String.format(
            "[%s]compatibility",
            hasCompatibilityChanged(schemaChange.subject, schemaChange.oldCompatibility) ? "*" : ""
        ),
        schemaChange.subject
            .getOptionalCompatibility()
            .orElse("")
    );
    return subjectInfo;
  }


  private static SchemaChange checkSchemaChanged(SchemaRegistryManager schemaRegistryManager,
                                                 Topic topic,
                                                 Subject subject) {
    // TODO: How to handle schema deletes

    // If the schema is already registered, only check compatibility
    ParsedSchema parsedSchema = subject
        .getOptionalSchemaFile()
        .map(schemaRegistryManager::schemaFilePath)
        .map(schemaPath ->
            schemaRegistryManager.readSchemaFile(subject.getFormat(), schemaPath)
        )
        .orElse(null);

    if (parsedSchema == null)
      return null;

    String subjectName = subject.buildSubjectName(topic);

    Integer schemaId = schemaRegistryManager.getId(subjectName, parsedSchema);

    String oldCompatibility =
        Optional
            .ofNullable(schemaRegistryManager.getCompatibility(subjectName))
            .map(String::trim)
            .filter(Predicate.not(String::isEmpty))
            .orElse(null);

    return (schemaId == null || hasCompatibilityChanged(subject, oldCompatibility))
        ? new SchemaChange(subject, subjectName, schemaId, parsedSchema, oldCompatibility)
        : null;
  }

  private static boolean hasCompatibilityChanged(Subject subject, String oldCompatibility) {
    return subject
        .getOptionalCompatibility()
        .filter(compatibility -> !compatibility.equalsIgnoreCase(oldCompatibility))
        .isPresent();
  }
}
