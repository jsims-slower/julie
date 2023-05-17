package com.purbon.kafka.topology.validation.topic;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.purbon.kafka.topology.exceptions.ConfigurationException;
import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ReplicationFactorValidationTest {

  @Test
  public void shouldVerifyDifferentValuesWhenUsingEq() throws ConfigurationException {
    Map<String, String> config = new HashMap<>();
    config.put("replication.factor", "34");
    config.put("num.partitions", "123");

    Topic topic = new Topic("topic", config);
    ReplicationFactorValidation validation = new ReplicationFactorValidation((short) 35, "eq");
    assertThrows(ValidationException.class, () -> validation.valid(topic));
  }

  @Test
  public void shouldVerifyDifferentValuesWhenUsingGte() throws ConfigurationException {
    Map<String, String> config = new HashMap<>();
    config.put("replication.factor", "34");
    config.put("num.partitions", "123");

    Topic topic = new Topic("topic", config);
    ReplicationFactorValidation validation = new ReplicationFactorValidation((short) 35, "gte");
    assertThrows(ValidationException.class, () -> validation.valid(topic));
  }

  @Test
  public void shouldVerifyDifferentValuesWhenUsingLte() throws ConfigurationException {
    Map<String, String> config = new HashMap<>();
    config.put("replication.factor", "34");
    config.put("num.partitions", "123");

    Topic topic = new Topic("topic", config);
    ReplicationFactorValidation validation = new ReplicationFactorValidation((short) 15, "lte");
    assertThrows(ValidationException.class, () -> validation.valid(topic));
  }

  @Test
  public void shouldVerifyDifferentValuesWhenUsingGt() throws ConfigurationException {
    Map<String, String> config = new HashMap<>();
    config.put("replication.factor", "34");
    config.put("num.partitions", "123");

    Topic topic = new Topic("topic", config);
    ReplicationFactorValidation validation = new ReplicationFactorValidation((short) 35, "gt");
    assertThrows(ValidationException.class, () -> validation.valid(topic));
  }

  @Test
  public void shouldVerifyDifferentValuesWhenUsingLt() throws ConfigurationException {
    Map<String, String> config = new HashMap<>();
    config.put("replication.factor", "34");
    config.put("num.partitions", "123");

    Topic topic = new Topic("topic", config);
    ReplicationFactorValidation validation = new ReplicationFactorValidation((short) 33, "lt");
    assertThrows(ValidationException.class, () -> validation.valid(topic));
  }

  @Test
  public void shouldVerifyDifferentValuesWhenUsingEqSuccessfully()
      throws ValidationException, ConfigurationException {
    Map<String, String> config = new HashMap<>();
    config.put("replication.factor", "34");
    config.put("num.partitions", "123");

    Topic topic = new Topic("topic", config);
    ReplicationFactorValidation validation = new ReplicationFactorValidation((short) 34, "eq");
    validation.valid(topic);
  }

  @Test
  public void shouldVerifyDifferentValuesWhenUsingGteSuccessfully()
      throws ValidationException, ConfigurationException {
    Map<String, String> config = new HashMap<>();
    config.put("replication.factor", "34");
    config.put("num.partitions", "123");

    Topic topic = new Topic("topic", config);
    ReplicationFactorValidation validation = new ReplicationFactorValidation((short) 34, "gte");
    validation.valid(topic);
  }

  @Test
  public void shouldVerifyDifferentValuesWhenUsingLteSuccessfully()
      throws ValidationException, ConfigurationException {
    Map<String, String> config = new HashMap<>();
    config.put("replication.factor", "34");
    config.put("num.partitions", "123");

    Topic topic = new Topic("topic", config);
    ReplicationFactorValidation validation = new ReplicationFactorValidation((short) 34, "lte");
    validation.valid(topic);
  }

  @Test
  public void shouldVerifyDifferentValuesWhenUsingGtSuccessfully()
      throws ValidationException, ConfigurationException {
    Map<String, String> config = new HashMap<>();
    config.put("replication.factor", "34");
    config.put("num.partitions", "123");

    Topic topic = new Topic("topic", config);
    ReplicationFactorValidation validation = new ReplicationFactorValidation((short) 33, "gt");
    validation.valid(topic);
  }

  @Test
  public void shouldVerifyDifferentValuesWhenUsingLtSuccessfully()
      throws ValidationException, ConfigurationException {
    Map<String, String> config = new HashMap<>();
    config.put("replication.factor", "34");
    config.put("num.partitions", "123");

    Topic topic = new Topic("topic", config);
    ReplicationFactorValidation validation = new ReplicationFactorValidation((short) 35, "lt");
    validation.valid(topic);
  }
}
