package com.purbon.kafka.topology.actions.topics;

import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.TestTopologyBuilder;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DeleteTopicsActionTest {

  @Mock TopologyBuilderAdminClient adminClient;

  @Test
  public void shouldComposeDetailedViewOfProperties() {
    Topic t = new Topic("foo");
    t.setConfig(Collections.singletonMap("foo", "bar"));

    TestTopologyBuilder builder = TestTopologyBuilder.createProject().addTopic(t);

    Topology topology = builder.buildTopology();
    var topic = topology.getProjects().get(0).getTopics().get(0);

    var action = new DeleteTopics(adminClient, Collections.singletonList(topic.toString()));

    assertThat(action.refs())
        .hasSize(1)
        .allSatisfy(
            ref -> {
              assertThat(ref)
                  .contains(
                      "\"resource_name\" : \"rn://delete.topic/com.purbon.kafka.topology.actions.topics.DeleteTopics/ctx.project.foo\"");
              assertThat(ref).contains("\"topic\" : \"ctx.project.foo\",");
            });
  }
}
