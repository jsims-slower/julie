package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.api.ksql.KsqlApiClient.STREAM_TYPE;
import static com.purbon.kafka.topology.api.ksql.KsqlApiClient.TABLE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.api.ksql.KsqlApiClient;
import com.purbon.kafka.topology.api.ksql.KsqlClientConfig;
import com.purbon.kafka.topology.integration.containerutils.ContainerFactory;
import com.purbon.kafka.topology.integration.containerutils.KsqlContainer;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class KsqlClientIT {
  @Container
  private static final SaslPlaintextKafkaContainer container =
      ContainerFactory.fetchSaslKafkaContainer(System.getProperty("cp.version"));

  @Container private static final KsqlContainer ksqlContainer = new KsqlContainer(container);

  @Test
  public void testStreamTableCreateAndDelete() throws IOException {

    KsqlApiClient client =
        new KsqlApiClient(KsqlClientConfig.builder().setServer(ksqlContainer.getUrl()).build());

    String streamName = "riderLocations";
    client.addSessionVars(Collections.singletonMap("partitions", "1"));
    String sql =
        "CREATE STREAM "
            + streamName
            + " (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE)\n"
            + "  WITH (kafka_topic='locations', value_format='json', partitions=${partitions});";

    client.add(sql);

    List<String> queries = client.list();
    assertThat(queries).hasSize(1);

    client.delete(streamName, STREAM_TYPE);

    queries = client.list();
    assertThat(queries).hasSize(0);

    String tableName = "users";
    sql =
        "CREATE TABLE "
            + tableName
            + " (\n"
            + "     id BIGINT PRIMARY KEY,\n"
            + "     usertimestamp BIGINT,\n"
            + "     gender VARCHAR,\n"
            + "     region_id VARCHAR\n"
            + "   ) WITH (\n"
            + "     KAFKA_TOPIC = 'my-users-topic', \n"
            + "     KEY_FORMAT='KAFKA', PARTITIONS=${partitions}, REPLICAS=1,"
            + "     VALUE_FORMAT = 'JSON'\n"
            + "   );";

    client.add(sql);

    queries = client.list();
    assertThat(queries).hasSize(1);

    client.delete(tableName, TABLE_TYPE);

    queries = client.list();
    assertThat(queries).hasSize(0);
  }
}
