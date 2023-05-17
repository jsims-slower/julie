package com.purbon.kafka.topology.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.Collections;
import org.apache.kafka.common.resource.ResourceType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;

@ExtendWith(MockitoExtension.class)
public class RedisBackendTest {

  @Mock Jedis jedis;

  private RedisBackend stateProcessor;
  private String bucket;

  @BeforeEach
  public void before() {
    bucket = "foo";
    stateProcessor = new RedisBackend(jedis, bucket);
  }

  @Test
  public void testSaveBindings() throws IOException {

    BackendState state = buildBackendState();
    stateProcessor.save(state);

    verify(jedis, times(1)).set(eq(bucket), anyString());
  }

  @Test
  public void testDataLoading() throws IOException {

    BackendState mockedState = buildBackendState();
    when(jedis.get(eq(bucket))).thenReturn(mockedState.asPrettyJson());

    BackendState state = stateProcessor.load();
    assertEquals(1, state.size());
    assertThat(state.getBindings()).first().returns("Topic A", TopologyAclBinding::getResourceName);
  }

  @NotNull
  private BackendState buildBackendState() {
    TopologyAclBinding binding =
        TopologyAclBinding.build(
            ResourceType.CLUSTER.name(), "Topic A", "host", "op", "principal", "LITERAL");

    BackendState state = new BackendState();
    state.addBindings(Collections.singleton(binding));
    return state;
  }
}
