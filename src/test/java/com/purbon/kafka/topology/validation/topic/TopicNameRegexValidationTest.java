package com.purbon.kafka.topology.validation.topic;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.purbon.kafka.topology.exceptions.ConfigurationException;
import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import org.junit.jupiter.api.Test;

public class TopicNameRegexValidationTest {

  @Test
  public void testKoConfigValues() throws ConfigurationException {
    Topic topic = new Topic("topic");
    TopicNameRegexValidation validation = new TopicNameRegexValidation("[1-9]");
    assertThrows(ValidationException.class, () -> validation.valid(topic));
  }

  @Test
  public void testOkConfigValues() throws ValidationException, ConfigurationException {
    Topic topic = new Topic("topic");
    TopicNameRegexValidation validation = new TopicNameRegexValidation("[a-z]*");
    validation.valid(topic);
  }

  @Test
  public void testEmptyParam() throws ConfigurationException {
    assertThrows(ConfigurationException.class, () -> new TopicNameRegexValidation(""));
  }
}
