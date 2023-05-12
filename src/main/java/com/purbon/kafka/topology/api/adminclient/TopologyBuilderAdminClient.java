package com.purbon.kafka.topology.api.adminclient;

import com.purbon.kafka.topology.actions.topics.TopicConfigUpdatePlan;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.acl.*;
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
      log.error("{}", e.getMessage(), e);
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
      log.error("Failed to update configs for topic " + configUpdatePlan.getFullTopicName(), ex);
      throw new RuntimeException(ex);
    }
  }

  public int getPartitionCount(String topic) throws IOException {
    try {
      Map<String, TopicDescription> results =
          adminClient.describeTopics(Collections.singletonList(topic)).all().get();
      return results.get(topic).partitions().size();
    } catch (InterruptedException | ExecutionException e) {
      log.error("{}", e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public void updatePartitionCount(Topic topic, String topicName) throws IOException {
    Map<String, NewPartitions> map = new HashMap<>();
    map.put(topicName, NewPartitions.increaseTo(topic.partitionsCount()));
    try {
      adminClient.createPartitions(map).all().get();
    } catch (InterruptedException | ExecutionException e) {
      log.error("{}", e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public void clearAcls() throws IOException {
    Collection<AclBindingFilter> filters = new ArrayList<>();
    filters.add(AclBindingFilter.ANY);
    clearAcls(filters);
  }

  public void clearAcls(TopologyAclBinding aclBinding) throws IOException {
    Collection<AclBindingFilter> filters = new ArrayList<>();

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
    filters.add(filter);
    clearAcls(filters);
  }

  private void clearAcls(Collection<AclBindingFilter> filters) throws IOException {
    try {
      adminClient.deleteAcls(filters).all().get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public Config getActualTopicConfig(String topic) {
    ConfigResource resource = new ConfigResource(Type.TOPIC, topic);
    Collection<ConfigResource> resources = Collections.singletonList(resource);

    final Map<ConfigResource, Config> configs;
    try {
      configs = adminClient.describeConfigs(resources).all().get();
    } catch (InterruptedException | ExecutionException ex) {
      log.error("{}", ex.getMessage(), ex);
      throw new RuntimeException(ex);
    }

    return configs.get(resource);
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
      log.error("{}", e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public void createTopic(String topicName) throws IOException {
    Topic topic = new Topic();
    createTopic(topic, topicName);
  }

  private void createAllTopics(Collection<NewTopic> newTopics)
      throws ExecutionException, InterruptedException {
    adminClient.createTopics(newTopics).all().get();
  }

  public void deleteTopics(Collection<String> topics) throws IOException {
    try {
      adminClient.deleteTopics(topics).all().get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public Map<String, Collection<AclBinding>> fetchAclsList() {
    Map<String, Collection<AclBinding>> acls = new HashMap<>();

    try {
      Collection<AclBinding> list = adminClient.describeAcls(AclBindingFilter.ANY).values().get();
      list.forEach(
          aclBinding -> {
            String name = aclBinding.pattern().name();
            Collection<AclBinding> updatedList = acls.computeIfAbsent(name, k -> new ArrayList<>());
            updatedList.add(aclBinding);
            acls.put(name, updatedList);
          });
    } catch (Exception e) {
      return new HashMap<>();
    }
    return acls;
  }

  public void createAcls(Collection<AclBinding> acls) {
    try {
      String aclsDump = acls.stream().map(AclBinding::toString).collect(Collectors.joining(", "));
      log.debug("createAcls: {}", aclsDump);
      adminClient.createAcls(acls).all().get();
    } catch (InvalidConfigurationException ex) {
      log.error("{}", ex.getMessage(), ex);
      throw ex;
    } catch (ExecutionException | InterruptedException e) {
      log.error("{}", e.getMessage(), e);
    }
  }

  public void close() {
    adminClient.close();
  }
}
