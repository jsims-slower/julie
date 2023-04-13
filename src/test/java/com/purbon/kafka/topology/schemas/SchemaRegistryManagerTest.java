package com.purbon.kafka.topology.schemas;

import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.schemas.SchemaRegistryManager.SchemaRegistryManagerException;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SchemaRegistryManagerTest {

  private static final String subjectName = "bananas";
  private static final String simpleSchema = "{\"type\": \"string\"}";

  private SchemaRegistryClient client;
  private SchemaRegistryManager manager;

  private Path rootDir;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void before() {
    List<SchemaProvider> providers =
        Arrays.asList(
            new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider());
    client = new MockSchemaRegistryClient(providers);
    rootDir = Paths.get(System.getProperty("user.dir"), "target", "test-classes");
    manager = new SchemaRegistryManager(client, rootDir.toString());
  }

  @Test
  public void shouldRegisterTheSchema() throws Exception {

    ParsedSchema parsedSchema = manager.parseSchema(AvroSchema.TYPE, simpleSchema);
    final int schemaId = manager.register(subjectName, parsedSchema);
    assertThat(schemaId).isEqualTo(1);

    assertThat(client.getAllSubjects()).hasSize(1).containsExactly(subjectName);
    assertThat(client.getAllVersions(subjectName)).hasSize(1).containsExactly(1);
  }

  @Test
  public void shouldRegisterTheSchemaWithDefaultAvroType() throws Exception {

    Path schemaFilePath =
        Paths.get(getClass().getClassLoader().getResource("schemas/bar-value.avsc").toURI());

    ParsedSchema parsedSchema = manager.readSchemaFile(AvroSchema.TYPE, schemaFilePath);
    final int schemaId = manager.register(subjectName, parsedSchema);
    assertThat(schemaId).isEqualTo(1);

    assertThat(client.getAllSubjects()).hasSize(1).containsExactly(subjectName);
    assertThat(client.getAllVersions(subjectName)).hasSize(1).containsExactly(1);
  }

  @Test
  public void shouldRegisterTheSchemaWithCompatibility()
      throws IOException, RestClientException, URISyntaxException {

    Path schemaFilePath =
        Paths.get(getClass().getClassLoader().getResource("schemas/bar-value.avsc").toURI());

    ParsedSchema parsedSchema = manager.readSchemaFile(AvroSchema.TYPE, schemaFilePath);
    final int schemaId = manager.register(subjectName, parsedSchema);
    assertThat(schemaId).isEqualTo(1);

    CompatibilityLevel compLevel = manager.setCompatibility(subjectName, CompatibilityLevel.FORWARD);
    assertThat(compLevel).isEqualTo(CompatibilityLevel.FORWARD);
    assertThat(client.getCompatibility(subjectName)).isEqualTo("FORWARD");
  }

  @Test(expected = SchemaRegistryManagerException.class)
  public void shouldThrowAnExceptionWithFailedFilePath() {
    Path schemaFilePath = manager.schemaFilePath("schemas/wrong-file-value.avsc");
    manager.readSchemaFile(AvroSchema.TYPE, schemaFilePath);
  }

  @Test
  public void shouldMakePathFromARelativePath() {
    Path expectedPath = Paths.get(rootDir.toString(), "schemas/bar-value.avsc");
    Path actualPath = manager.schemaFilePath( "schemas/bar-value.avsc");
    assertThat(actualPath).isEqualTo(expectedPath);
  }

  @Test
  public void shouldRegisterAndUpdateAvroSchema() throws Exception {

    Path schemaFilePath =
        Paths.get(getClass().getClassLoader().getResource("schemas/test.avsc").toURI());
    ParsedSchema parsedSchema = manager.readSchemaFile(AvroSchema.TYPE, schemaFilePath);
    assertThat(manager.register(subjectName, parsedSchema)).isEqualTo(1);

    Path updatedSchemaFilePath =
        Paths.get(
            getClass()
                .getClassLoader()
                .getResource("schemas/test-backward-compatible.avsc")
                .toURI());

    final ParsedSchema parsedUpdatedUserSchema =
        manager.readSchemaFile(AvroSchema.TYPE, updatedSchemaFilePath);
    assertThat(client.testCompatibility(subjectName, parsedUpdatedUserSchema)).isTrue();

    assertThat(manager.register(subjectName, parsedUpdatedUserSchema)).isEqualTo(2);

    assertThat(client.getAllSubjects()).hasSize(1).containsExactly(subjectName);
    assertThat(client.getAllVersions(subjectName)).hasSize(2).containsExactly(1, 2);
  }

  @Test
  public void shouldDetectIncompatibleAvroSchema()
      throws URISyntaxException, IOException, RestClientException {

    Path schemaFilePath =
        Paths.get(getClass().getClassLoader().getResource("schemas/test.avsc").toURI());
    ParsedSchema parsedSchema = manager.readSchemaFile(AvroSchema.TYPE, schemaFilePath);
    assertThat(manager.register(subjectName, parsedSchema)).isEqualTo(1);
    manager.setCompatibility(subjectName, CompatibilityLevel.FORWARD);
    assertThat(client.getCompatibility(subjectName)).isEqualTo("FORWARD");

    Path updatedSchemaFilePath =
        Paths.get(
            getClass()
                .getClassLoader()
                .getResource("schemas/test-backward-compatible.avsc")
                .toURI());

    final ParsedSchema parsedUpdatedSampleSchema =
        manager.readSchemaFile(AvroSchema.TYPE, updatedSchemaFilePath);
    assertThat(client.testCompatibility(subjectName, parsedUpdatedSampleSchema)).isFalse();
  }

  @Test
  public void shouldRegisterAndUpdateJsonSchema() throws Exception {

    Path schemaFilePath =
        Paths.get(getClass().getClassLoader().getResource("schemas/test.json").toURI());
    ParsedSchema parsedSchema = manager.readSchemaFile(JsonSchema.TYPE, schemaFilePath);
    assertThat(manager.register(subjectName, parsedSchema)).isEqualTo(1);

    Path updatedSchemaFilePath =
        Paths.get(
            getClass()
                .getClassLoader()
                .getResource("schemas/test-forward-compatible.json")
                .toURI());

    final ParsedSchema parsedUpdatedSampleSchema =
        manager.readSchemaFile(JsonSchema.TYPE, updatedSchemaFilePath);

    assertThat(client.testCompatibility(subjectName, parsedUpdatedSampleSchema)).isTrue();

    assertThat(manager.register(subjectName, parsedUpdatedSampleSchema)).isEqualTo(2);

    assertThat(client.getAllSubjects()).hasSize(1).containsExactly(subjectName);
    assertThat(client.getAllVersions(subjectName)).hasSize(2).containsExactly(1, 2);
  }

  @Test
  public void shouldDetectIncompatibleJsonSchema()
      throws URISyntaxException, IOException, RestClientException {

    Path schemaFilePath =
        Paths.get(getClass().getClassLoader().getResource("schemas/test.json").toURI());
    ParsedSchema parsedSchema = manager.readSchemaFile(JsonSchema.TYPE, schemaFilePath);
    assertThat(manager.register(subjectName, parsedSchema)).isEqualTo(1);

    Path updatedSchemaFilePath =
        Paths.get(
            getClass()
                .getClassLoader()
                .getResource("schemas/test-backward-compatible.json")
                .toURI());

    final ParsedSchema parsedUpdatedSampleSchema =
        manager.readSchemaFile(JsonSchema.TYPE, updatedSchemaFilePath);
    assertThat(client.testCompatibility(subjectName, parsedUpdatedSampleSchema)).isFalse();
  }

  @Test
  public void shouldRegisterAndUpdateProtobufSchema() throws Exception {

    Path schemaFilePath =
        Paths.get(getClass().getClassLoader().getResource("schemas/test.proto").toURI());
    ParsedSchema parsedSchema = manager.readSchemaFile(ProtobufSchema.TYPE, schemaFilePath);
    assertThat(manager.register(subjectName, parsedSchema)).isEqualTo(1);

    Path updatedSchemaFilePath =
        Paths.get(
            getClass()
                .getClassLoader()
                .getResource("schemas/test-backward-compatible.proto")
                .toURI());

    final ParsedSchema parsedUpdatedSampleSchema =
        manager.readSchemaFile(ProtobufSchema.TYPE, updatedSchemaFilePath);
    assertThat(client.testCompatibility(subjectName, parsedUpdatedSampleSchema)).isTrue();

    assertThat(manager.register(subjectName, parsedUpdatedSampleSchema)).isEqualTo(2);

    assertThat(client.getAllSubjects()).hasSize(1).containsExactly(subjectName);
    assertThat(client.getAllVersions(subjectName)).hasSize(2).containsExactly(1, 2);
  }

  @Test
  public void shouldDetectIncompatibleProtobufSchema()
      throws URISyntaxException, IOException, RestClientException {

    Path schemaFilePath =
        Paths.get(getClass().getClassLoader().getResource("schemas/test.proto").toURI());
    ParsedSchema parsedSchema = manager.readSchemaFile(ProtobufSchema.TYPE, schemaFilePath);
    assertThat(manager.register(subjectName, parsedSchema)).isEqualTo(1);

    Path updatedSchemaFilePath =
        Paths.get(
            getClass()
                .getClassLoader()
                .getResource("schemas/test-forward-compatible.proto")
                .toURI());

    final ParsedSchema parsedUpdatedSampleSchema =
        manager.readSchemaFile(ProtobufSchema.TYPE, updatedSchemaFilePath);
    assertThat(client.testCompatibility(subjectName, parsedUpdatedSampleSchema)).isFalse();
  }

  @Test(expected = SchemaRegistryManager.SchemaRegistryManagerException.class)
  public void shouldFailForTheUnknownType() {

    final String unknownSchemaType = "bunch-of-monkeys";
    manager.parseSchema(unknownSchemaType, simpleSchema);
  }
}
