package com.purbon.kafka.topology.validation.topic;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.validation.TopicValidation;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.TopicConfig;

/**
 * This validation checks that all topic configs are valid ones according to the TopicConfig class.
 */
@Slf4j
public class ConfigurationKeyValidation implements TopicValidation {

  public static final Set<String> allowedConfigKeys =
      Arrays.stream(TopicConfig.class.getDeclaredFields())
          .filter(field -> Modifier.isStatic(field.getModifiers()))
          .filter(field -> field.getType().isAssignableFrom(String.class))
          .map(
              field -> {
                try {
                  return field.get(null);
                } catch (IllegalAccessException ex) {
                  throw new RuntimeException(ex);
                }
              })
          .map(String.class::cast)
          .collect(Collectors.toSet());

  @Override
  public void valid(Topic topic) throws ValidationException {
    // TODO: is topic.clone() necessary?
    Set<String> configuredKeys = new HashSet<>(topic.clone().getRawConfig().keySet());
    configuredKeys.removeAll(allowedConfigKeys);
    for (String invalidKey : configuredKeys) {
      throw new ValidationException(
          String.format("Topic %s has an invalid configuration value: %s", topic, invalidKey));
    }
  }
}
