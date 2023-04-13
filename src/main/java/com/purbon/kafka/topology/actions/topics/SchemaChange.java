package com.purbon.kafka.topology.actions.topics;

import com.purbon.kafka.topology.model.schema.Subject;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

@RequiredArgsConstructor
final class SchemaChange {
  public static Comparator<SchemaChange> comparator =
      Comparator.comparing(SchemaChange::getSubjectName);

  private final SchemaRegistryManager schemaRegistryManager;
  private final Subject subject;
  @Getter
  private final String subjectName;
  private final Integer schemaId;
  private final ParsedSchema parsedSchema;
  private final CompatibilityLevel oldCompatibility;

  public static SchemaChange createIfHasChanges(SchemaRegistryManager schemaRegistryManager,
                                                Subject subject,
                                                String subjectName) {
    // TODO: How do we want to handle schema deletes?

    return subject
        .getOptionalSchemaFile()
        .map(schemaRegistryManager::schemaFilePath)
        .map(schemaPath ->
            schemaRegistryManager.readSchemaFile(subject.getFormat(), schemaPath)
        )
        .map(parsedSchema -> new SchemaChange(
            schemaRegistryManager,
            subject,
            subjectName,
            schemaRegistryManager.getId(subjectName, parsedSchema),
            parsedSchema,
            schemaRegistryManager.getCompatibility(subjectName)
        ))
        // Even if the schema is already registered, we still need to check compatibility
        .filter(schemaChange -> schemaChange.schemaId == null || schemaChange.hasCompatibilityChanged())
        .orElse(null);
  }

  public void applyChanges() {
    if (schemaId == null) {
      registerSchema();
    }
    if (hasCompatibilityChanged()) {
      updateCompatibility();
    }
  }

  public Map<String, String> toProps() {
    Map<String, String> subjectInfo = new LinkedHashMap<>();
    subjectInfo.put(
        String.format("[%s]file", schemaId == null ? "*" : ""),
        subject.getOptionalSchemaFile().orElse("")
    );
    subjectInfo.put("format", subject.getFormat());
    subjectInfo.put(
        String.format("[%s]compatibility", hasCompatibilityChanged() ? "*" : ""),
        subject.getOptionalCompatibility().map(Enum::name).orElse("")
    );
    return subjectInfo;
  }

  private boolean hasCompatibilityChanged() {
    return subject
        .getOptionalCompatibility()
        .filter(compatibility -> compatibility != oldCompatibility)
        .isPresent();
  }

  private void registerSchema() {
    schemaRegistryManager.register(subjectName, parsedSchema);
  }

  private void updateCompatibility() {
    subject
        .getOptionalCompatibility()
        .ifPresent(compatibilityLevel ->
            schemaRegistryManager.setCompatibility(subjectName, compatibilityLevel)
        );
  }
}
