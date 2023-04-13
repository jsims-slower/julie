package com.purbon.kafka.topology.model.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.purbon.kafka.topology.model.Topic;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import java.util.Optional;
import java.util.function.Predicate;

public class Subject {

  private final Optional<String> schemaFile;
  private final Optional<String> recordType;
  private final Optional<CompatibilityLevel> optionalCompatibility;
  private final Optional<String> optionalFormat;
  private final SubjectKind kind;

  public enum SubjectKind {
    KEY("key"),
    VALUE("value");

    private final String label;

    SubjectKind(String label) {
      this.label = label;
    }
  }

  public Subject(
      Optional<JsonNode> schemaFileJsonNode,
      Optional<JsonNode> recordTypeJsonNode,
      Optional<JsonNode> optionalFormat,
      Optional<JsonNode> optionalCompatibility,
      SubjectKind kind) {
    this.schemaFile = schemaFileJsonNode
        .map(JsonNode::asText)
        .map(String::trim)
        .filter(Predicate.not(String::isEmpty));
    this.recordType = recordTypeJsonNode
        .map(JsonNode::asText)
        .map(String::trim)
        .filter(Predicate.not(String::isEmpty));
    this.optionalCompatibility = optionalCompatibility
        .map(JsonNode::asText)
        .map(String::trim)
        .filter(Predicate.not(String::isEmpty))
        .map(compatibility -> Optional
            .ofNullable(CompatibilityLevel.forName(compatibility))
            .orElseThrow(() -> new RuntimeException(
                "Invalid CompatibilityLevel [" + compatibility + "]"
            ))
        );
    this.optionalFormat = optionalFormat
        .map(JsonNode::asText)
        .map(String::trim)
        .filter(Predicate.not(String::isEmpty));
    this.kind = kind;
  }

  public Subject(String schemaFile, String recordType, SubjectKind kind) {
    this.schemaFile = Optional
        .ofNullable(schemaFile)
        .map(String::trim)
        .filter(Predicate.not(String::isEmpty));
    this.recordType = Optional
        .ofNullable(recordType)
        .map(String::trim)
        .filter(Predicate.not(String::isEmpty));
    this.optionalCompatibility = Optional.empty();
    this.optionalFormat = Optional.empty();
    this.kind = kind;
  }

  public Optional<String> getOptionalSchemaFile() {
    return schemaFile;
  }

  private String recordTypeAsString() {
    return recordType.orElseThrow(() -> new RuntimeException("Missing record type for " + schemaFile));
  }

  public String getFormat() {
    return optionalFormat.orElse(AvroSchema.TYPE);
  }

  public Optional<CompatibilityLevel> getOptionalCompatibility() {
    return optionalCompatibility;
  }

  public String buildSubjectName(Topic topic) {
    switch (topic.getSubjectNameStrategy()) {
      case TOPIC_NAME_STRATEGY:
        return topic + "-" + kind.label;
      case RECORD_NAME_STRATEGY:
        return recordTypeAsString();
      case TOPIC_RECORD_NAME_STRATEGY:
        return topic + "-" + recordTypeAsString();
      default:
        return "";
    }
  }
}
