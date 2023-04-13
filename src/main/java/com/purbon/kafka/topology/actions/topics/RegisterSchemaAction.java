package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.actions.BaseAction;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.schema.TopicSchemas;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j2
@RequiredArgsConstructor
public class RegisterSchemaAction extends BaseAction {

  private final Topic topic;
  private final String fullTopicName;
  private final List<SchemaChange> schemaChanges;

  public static RegisterSchemaAction createIfHasChanges(SchemaRegistryManager schemaRegistryManager,
                                                        Topic topic,
                                                        String fullTopicName) {
    List<SchemaChange> schemaChanges = new LinkedList<>();
    for (TopicSchemas schema : topic.getSchemas()) {
      Stream
          .of(schema.getKeySubject(), schema.getValueSubject())
          .filter(Objects::nonNull)
          .map(subject -> SchemaChange.createIfHasChanges(
              schemaRegistryManager,
              subject,
              subject.buildSubjectName(topic)
          ))
          .filter(Objects::nonNull)
          .forEach(schemaChanges::add);
    }
    return schemaChanges.isEmpty()
        ? null
        : new RegisterSchemaAction(topic, fullTopicName, schemaChanges);
  }

  @Override
  public void run() {
    log.debug(String.format("Register schemas for topic %s", fullTopicName));
    schemaChanges.forEach(SchemaChange::applyChanges);
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new LinkedHashMap<>();
    Map<String, ?> schemas =
        schemaChanges
            .stream()
            .sorted(SchemaChange.comparator)
            .collect(Collectors.toMap(
                SchemaChange::getSubjectName,
                SchemaChange::toProps,
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
                      schemaChange.getSubjectName().equalsIgnoreCase(keySubjectName)
                  ),
                  schemaChanges.stream().filter(schemaChange ->
                      schemaChange.getSubjectName().equalsIgnoreCase(valueSubjectName)
                  )
              )
              .collect(Collectors.toMap(
                  SchemaChange::getSubjectName,
                  SchemaChange::toProps,
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
}
