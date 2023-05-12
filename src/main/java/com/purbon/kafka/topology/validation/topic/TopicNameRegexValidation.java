package com.purbon.kafka.topology.validation.topic;

import static com.purbon.kafka.topology.Constants.TOPOLOGY_VALIDATIONS_TOPIC_NAME_REGEXP;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.exceptions.ConfigurationException;
import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.validation.TopicValidation;
import com.typesafe.config.ConfigException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class TopicNameRegexValidation implements TopicValidation {

  private final String topicNamePattern;

  public TopicNameRegexValidation(Configuration config) throws ConfigurationException {
    this(getTopicNamePatternFromConfig(config));
  }

  public TopicNameRegexValidation(String pattern) throws ConfigurationException {
    validateRegexpPattern(pattern);

    this.topicNamePattern = pattern;
  }

  @Override
  public void valid(Topic topic) throws ValidationException {
    log.trace("Applying Topic Name Regex Validation [{}]", topicNamePattern);

    if (!topic.getName().matches(topicNamePattern)) {
      String msg =
          String.format("Topic name '%s' does not follow regex: %s", topic, topicNamePattern);
      throw new ValidationException(msg);
    }
  }

  private static String getTopicNamePatternFromConfig(Configuration config)
      throws ConfigurationException {
    try {
      return config.getProperty(TOPOLOGY_VALIDATIONS_TOPIC_NAME_REGEXP);
    } catch (ConfigException e) {
      String msg =
          String.format(
              "TopicNameRegexValidation requires you to define your regex in config '%s'",
              TOPOLOGY_VALIDATIONS_TOPIC_NAME_REGEXP);
      throw new ConfigurationException(msg);
    }
  }

  private void validateRegexpPattern(String pattern) throws ConfigurationException {
    if (StringUtils.isBlank(pattern)) {
      throw new ConfigurationException(
          "TopicNameRegexValidation is configured without specifying a topic name pattern. Use config 'topology.validations.regexp'");
    }

    try {
      Pattern.compile(pattern);
    } catch (PatternSyntaxException exception) {
      throw new ConfigurationException(
          String.format("TopicNameRegexValidation configured with invalid regex '%s'", pattern));
    }
  }
}
