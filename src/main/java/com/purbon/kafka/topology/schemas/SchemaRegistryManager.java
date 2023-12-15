package com.purbon.kafka.topology.schemas;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaRegistryManager {
  /**
   * @see io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
   */
  public static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;

  public static final int VERSION_NOT_FOUND_ERROR_CODE = 40402;
  public static final int SCHEMA_NOT_FOUND_ERROR_CODE = 40403;
  public static final Set<Integer> NOT_FOUND_ERROR_CODES =
      Set.of(
          SUBJECT_NOT_FOUND_ERROR_CODE, VERSION_NOT_FOUND_ERROR_CODE, SCHEMA_NOT_FOUND_ERROR_CODE);

  static class SchemaRegistryManagerException extends RuntimeException {
    public SchemaRegistryManagerException(String message) {
      super(message);
    }

    public SchemaRegistryManagerException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private final SchemaRegistryClient schemaRegistryClient;
  private final String rootPath;

  public SchemaRegistryManager(
      SchemaRegistryClient schemaRegistryClient, String topologyFileOrDir) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.rootPath =
        Files.isDirectory(Paths.get(topologyFileOrDir))
            ? topologyFileOrDir
            : new File(topologyFileOrDir).getParent();
  }

  public int register(String subjectName, ParsedSchema parsedSchema) {
    log.debug("Registering subject {} with type {}", subjectName, parsedSchema.schemaType());
    try {
      return schemaRegistryClient.register(subjectName, parsedSchema);
    } catch (Exception e) {
      final String msg =
          String.format(
              "Failed to register the schema for subject '%s' of type '%s'",
              subjectName, parsedSchema.schemaType());
      throw new SchemaRegistryManagerException(msg, e);
    }
  }

  public Integer getId(String subjectName, ParsedSchema schema) {
    log.debug("Looking up subject [{}] based on schema: {}", subjectName, schema);

    try {
      return schemaRegistryClient.getId(subjectName, schema);
    } catch (RestClientException rce) {
      if (NOT_FOUND_ERROR_CODES.contains(rce.getErrorCode())) return null;
      final String msg =
          String.format(
              "Failed to lookup schema for subject [%s] based on schema: %s", subjectName, schema);
      throw new SchemaRegistryManagerException(msg, rce);
    } catch (Exception ex) {
      final String msg =
          String.format(
              "Failed to lookup schema for subject [%s] based on schema: %s", subjectName, schema);
      throw new SchemaRegistryManagerException(msg, ex);
    }
  }

  public CompatibilityLevel setCompatibility(String subjectName, CompatibilityLevel compatibility) {
    try {
      return Optional.ofNullable(
              schemaRegistryClient.updateCompatibility(subjectName, compatibility.name()))
          .map(String::trim)
          .filter(Predicate.not(String::isEmpty))
          .map(String::toUpperCase)
          .map(CompatibilityLevel::valueOf)
          .orElse(null);
    } catch (Exception e) {
      final String msg =
          String.format(
              "Failed to register the schema compatibility mode '%s' for subject '%s'",
              compatibility.name(), subjectName);
      throw new SchemaRegistryManagerException(msg, e);
    }
  }

  public CompatibilityLevel getCompatibility(String subjectName) {
    log.debug("Looking up compatibility based on subject [{}]", subjectName);

    try {
      return Optional.ofNullable(schemaRegistryClient.getCompatibility(subjectName))
          .map(String::trim)
          .filter(Predicate.not(String::isEmpty))
          .map(String::toUpperCase)
          .map(CompatibilityLevel::valueOf)
          .orElse(null);
    } catch (RestClientException rce) {
      if (NOT_FOUND_ERROR_CODES.contains(rce.getErrorCode())) return null;
      final String msg =
          String.format("Failed to get schema compatibility mode for subject '%s'", subjectName);
      throw new SchemaRegistryManagerException(msg, rce);
    } catch (Exception ex) {
      final String msg =
          String.format("Failed to get schema compatibility mode for subject '%s'", subjectName);
      throw new SchemaRegistryManagerException(msg, ex);
    }
  }

  public Path schemaFilePath(String schemaFile) {
    try {
      Path mayBeAbsolutePath = Paths.get(schemaFile);
      Path path =
          mayBeAbsolutePath.isAbsolute() ? mayBeAbsolutePath : Paths.get(rootPath, schemaFile);
      log.debug("Loading SchemaFile {} from path {}", schemaFile, path);
      return path;
    } catch (Exception ex) {
      throw new SchemaRegistryManagerException("Failed to find the schema file " + schemaFile, ex);
    }
  }

  public ParsedSchema readSchemaFile(String schemaType, Path schemaPath) {
    final String schemaString;
    try {
      log.debug("Reading schema from schema path {}", schemaPath);
      schemaString = Files.readString(schemaPath, StandardCharsets.UTF_8);
    } catch (Exception ex) {
      throw new SchemaRegistryManagerException("Failed to read schema file " + schemaPath, ex);
    }

    return parseSchema(schemaType, schemaString);
  }

  public ParsedSchema parseSchema(String schemaType, String schemaString) {
    Optional<String> schemaStringOpt =
        Optional.ofNullable(schemaString).map(String::trim).filter(Predicate.not(String::isEmpty));

    try {
      log.debug(
          "Parsing schema of type [{}] length [{}]",
          schemaType,
          schemaStringOpt.map(String::length).orElse(-1));

      return schemaStringOpt
          .flatMap(
              schemaString_ ->
                  schemaRegistryClient.parseSchema(
                      schemaType, schemaString_, Collections.emptyList()))
          .orElseThrow(
              () ->
                  new SchemaRegistryManagerException(
                      String.format(
                          "Failed to parse schema of type [%s] length [%d]",
                          schemaType, schemaStringOpt.map(String::length).orElse(-1))));
    } catch (SchemaRegistryManagerException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new SchemaRegistryManagerException(
          String.format(
              "Failed to parse schema of type [%s] length [%d]",
              schemaType, schemaStringOpt.map(String::length).orElse(-1)),
          ex);
    }
  }
}
