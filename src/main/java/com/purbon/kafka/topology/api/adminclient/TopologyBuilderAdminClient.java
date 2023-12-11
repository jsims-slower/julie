package com.purbon.kafka.topology.api.adminclient;

import com.purbon.kafka.topology.actions.topics.TopicConfigUpdatePlan;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

@Slf4j
@RequiredArgsConstructor
public class TopologyBuilderAdminClient {

  private final AdminClient adminClient;

  public Set<String> listTopics(ListTopicsOptions options) throws IOException {
    Set<String> listOfTopics;
    try {
      listOfTopics = adminClient.listTopics(options).names().get();
    } catch (InterruptedException | ExecutionException e) {
      log.error(e.getMessage(), e);
      throw new IOException(e);
    }
    return listOfTopics;
  }

  public void healthCheck() throws IOException {

    try {
      adminClient.describeCluster().nodes().get();
    } catch (Exception ex) {
      throw new IOException("Problem during the health-check operation", ex);
    }
  }

  public Set<String> listTopics() throws IOException {
    return listTopics(new ListTopicsOptions());
  }

  public Set<String> listApplicationTopics() throws IOException {
    ListTopicsOptions options = new ListTopicsOptions();
    options.listInternal(false);
    return listTopics(options);
  }

  public void updateTopicConfig(TopicConfigUpdatePlan configUpdatePlan) {
    Set<AlterConfigOp> configChanges = new HashSet<>();

    configUpdatePlan
        .getNewConfigValues()
        .forEach(
            (configKey, configValue) ->
                configChanges.add(
                    new AlterConfigOp(new ConfigEntry(configKey, configValue), OpType.SET)));

    configUpdatePlan
        .getUpdatedConfigValues()
        .forEach(
            (configKey, configValuePair) ->
                configChanges.add(
                    new AlterConfigOp(
                        new ConfigEntry(configKey, configValuePair.getRight()), OpType.SET)));

    configUpdatePlan
        .getDeletedConfigValues()
        .forEach(
            (configKey, configValue) ->
                configChanges.add(
                    new AlterConfigOp(new ConfigEntry(configKey, configValue), OpType.DELETE)));
    Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
    configs.put(new ConfigResource(Type.TOPIC, configUpdatePlan.getFullTopicName()), configChanges);

    try {
      adminClient.incrementalAlterConfigs(configs).all().get();
    } catch (InterruptedException | ExecutionException ex) {
      log.error("Failed to update configs for topic {}", configUpdatePlan.getFullTopicName(), ex);
      throw new RuntimeException(ex);
    }
  }

  public int getPartitionCount(String topic) throws IOException {
    try {
      return adminClient
          .describeTopics(Collections.singletonList(topic))
          .allTopicNames()
          .get()
          .get(topic)
          .partitions()
          .size();
    } catch (InterruptedException | ExecutionException e) {
      log.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public void updatePartitionCount(Topic topic, String topicName) throws IOException {
    Map<String, NewPartitions> map =
        Collections.singletonMap(topicName, NewPartitions.increaseTo(topic.partitionsCount()));
    try {
      adminClient.createPartitions(map).all().get();
    } catch (InterruptedException | ExecutionException e) {
      log.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public void clearAcls() throws IOException {
    clearAcls(Collections.singletonList(AclBindingFilter.ANY));
  }

  public void clearAcls(TopologyAclBinding aclBinding) throws IOException {
    log.debug("clearAcl = {}", aclBinding);
    ResourcePatternFilter resourceFilter =
        new ResourcePatternFilter(
            ResourceType.valueOf(aclBinding.getResourceType()),
            aclBinding.getResourceName(),
            PatternType.valueOf(aclBinding.getPattern()));

    AccessControlEntryFilter accessControlEntryFilter =
        new AccessControlEntryFilter(
            aclBinding.getPrincipal(),
            aclBinding.getHost(),
            AclOperation.valueOf(aclBinding.getOperation()),
            AclPermissionType.ANY);

    AclBindingFilter filter = new AclBindingFilter(resourceFilter, accessControlEntryFilter);
    clearAcls(Collections.singletonList(filter));
  }

  private void clearAcls(Collection<AclBindingFilter> filters) throws IOException {
    try {
      adminClient.deleteAcls(filters).all().get();
    } catch (ExecutionException | InterruptedException e) {
      log.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public Config getActualTopicConfig(String topic) {
    ConfigResource resource = new ConfigResource(Type.TOPIC, topic);
    Collection<ConfigResource> resources = Collections.singletonList(resource);

    try {
      return adminClient.describeConfigs(resources).all().get().get(resource);
    } catch (InterruptedException | ExecutionException ex) {
      log.error(ex.getMessage(), ex);
      throw new RuntimeException(ex);
    }
  }

  public void createTopic(Topic topic, String fullTopicName) throws IOException {
    NewTopic newTopic =
        new NewTopic(fullTopicName, topic.getPartitionCount(), topic.replicationFactor())
            .configs(topic.getRawConfig());
    try {
      createAllTopics(Collections.singleton(newTopic));
    } catch (ExecutionException | InterruptedException e) {
      if (e.getCause() instanceof TopicExistsException) {
        log.info(e.getMessage());
        return;
      }
      log.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public void createTopic(String topicName) throws IOException {
    createTopic(new Topic(), topicName);
  }

  private void createAllTopics(Collection<NewTopic> newTopics)
      throws ExecutionException, InterruptedException {
    adminClient.createTopics(newTopics).all().get();
  }

  public void deleteTopics(Collection<String> topics) throws IOException {
    try {
      adminClient.deleteTopics(topics).all().get();
    } catch (ExecutionException | InterruptedException e) {
      log.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public Map<String, Collection<AclBinding>> fetchAclsList() {
    try {
      return adminClient.describeAcls(AclBindingFilter.ANY).values().get().stream()
          .collect(
              Collectors.groupingBy(
                  aclBinding -> aclBinding.pattern().name(),
                  Collectors.toCollection(ArrayList::new)));
    } catch (Exception e) {
      return new HashMap<>();
    }
  }

  public void createAcls(Collection<AclBinding> acls) {
    try {
      String aclsDump = acls.stream().map(AclBinding::toString).collect(Collectors.joining(", "));
      log.debug("createAcls: {}", aclsDump);
      adminClient.createAcls(acls).all().get();
    } catch (InvalidConfigurationException ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    } catch (ExecutionException | InterruptedException e) {
      log.error(e.getMessage(), e);
    }
  }

  public void close() {
    adminClient.close();
  }
}
