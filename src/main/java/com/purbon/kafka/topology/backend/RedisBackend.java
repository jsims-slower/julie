package com.purbon.kafka.topology.backend;

import com.purbon.kafka.topology.BackendController.Mode;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.utils.JSON;
import java.io.IOException;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

@Slf4j
@RequiredArgsConstructor
public class RedisBackend implements Backend {

  private final Jedis jedis;
  private final String bucket;

  public RedisBackend(String host, int port, String bucket) {
    this(new Jedis(host, port), bucket);
  }

  public RedisBackend(Configuration config) {
    this(config.getRedisHost(), config.getRedisPort(), config.getRedisBucket());
  }

  @Override
  public void createOrOpen() {
    createOrOpen(Mode.APPEND);
  }

  @Override
  public void createOrOpen(Mode mode) {
    jedis.connect();
    if (mode.equals(Mode.TRUNCATE)) {
      jedis.del(bucket);
    }
  }

  @Override
  public void close() {
    jedis.close();
  }

  @Override
  public void save(BackendState state) throws IOException {
    log.debug("Storing state for: {}", state);
    jedis.set(bucket, state.asPrettyJson());
  }

  @Override
  public BackendState load() throws IOException {
    connectIfNeed();
    Optional<String> contentOptional = Optional.ofNullable(jedis.get(bucket));
    log.debug("Loading a new state instance: {}", contentOptional);
    return JSON.toObject(contentOptional.orElse("{}"), BackendState.class);
  }

  private void connectIfNeed() {
    if (!jedis.isConnected()) {
      createOrOpen();
    }
  }
}
