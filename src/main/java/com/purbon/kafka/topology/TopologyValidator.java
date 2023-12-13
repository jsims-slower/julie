package com.purbon.kafka.topology;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.validation.TopicValidation;
import com.purbon.kafka.topology.validation.TopologyValidation;
import com.purbon.kafka.topology.validation.Validation;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TopologyValidator {

  private final Configuration config;

  public List<String> validate(Topology topology) {

    var streamOfTopologyResults =
        validations().stream()
            .filter(p -> p instanceof TopologyValidation)
            .map(TopologyValidation.class::cast)
            .flatMap(
                validation -> {
                  try {
                    validation.valid(topology);
                    return Stream.empty();
                  } catch (ValidationException validationError) {
                    return Stream.of(validationError);
                  }
                });

    List<TopicValidation> listOfTopicValidations =
        validations().stream()
            .filter(p -> p instanceof TopicValidation)
            .map(TopicValidation.class::cast)
            .collect(Collectors.toList());

    Stream<Topic> streamOfTopics =
        topology.getProjects().stream().map(Project::getTopics).flatMap(Collection::stream);

    var streamOfTopicResults =
        streamOfTopics.flatMap(
            topic ->
                listOfTopicValidations.stream()
                    .flatMap(
                        validation -> {
                          try {
                            validation.valid(topic);
                            return Stream.empty();
                          } catch (ValidationException ex) {
                            return Stream.of(ex);
                          }
                        }));

    return Stream.concat(streamOfTopologyResults, streamOfTopicResults)
        .map(ValidationException::getMessage)
        .collect(Collectors.toList());
  }

  private List<Validation> validations() {
    return config.getTopologyValidations().stream()
        .map(
            validationClass -> {
              try {
                Class<?> clazz = getValidationClazz(validationClass);
                if (clazz == null) {
                  throw new IOException(
                      String.format(
                          "Could not find validation class '%s' in class path. "
                              + "Please use the fully qualified class name and check your config.",
                          validationClass));
                }

                Object instance;
                try {
                  Constructor<?> configurationConstructor =
                      clazz.getConstructor(Configuration.class);
                  instance = configurationConstructor.newInstance(config);
                } catch (NoSuchMethodException e) {
                  /* Support pre 4.1.1 validators with no-arg constructors. */
                  Constructor<?> noArgConstructor = clazz.getConstructor();
                  instance = noArgConstructor.newInstance();
                }
                if (instance instanceof TopologyValidation) {
                  return (TopologyValidation) instance;
                } else if (instance instanceof TopicValidation) {
                  return (TopicValidation) instance;
                } else {
                  throw new IOException("invalid validation type specified " + validationClass);
                }
              } catch (Exception ex) {
                throw new IllegalStateException(
                    "Failed to load topology validations from class path", ex);
              }
            })
        .collect(Collectors.toList());
  }

  private Class<?> getValidationClazz(String validationClass) {
    try {
      return Class.forName(validationClass);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
}
