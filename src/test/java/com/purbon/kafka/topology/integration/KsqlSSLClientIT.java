package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.api.ksql.KsqlApiClient.STREAM_TYPE;
import static com.purbon.kafka.topology.api.ksql.KsqlApiClient.TABLE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.api.ksql.KsqlApiClient;
import com.purbon.kafka.topology.api.ksql.KsqlClientConfig;
import com.purbon.kafka.topology.integration.containerutils.ContainerFactory;
import com.purbon.kafka.topology.integration.containerutils.KsqlContainer;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import com.purbon.kafka.topology.integration.containerutils.SslKsqlContainer;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class KsqlSSLClientIT {

  private static final String KSQLDB_TRUSTSTORE_JKS = "/ksql-ssl/truststore/ksqldb.truststore.jks";
  private static final String KSQLDB_KEYSTORE_JKS = "/ksql-ssl/keystore/ksqldb.keystore.jks";

  @Container
  private static final SaslPlaintextKafkaContainer container =
      ContainerFactory.fetchSaslKafkaContainer(System.getProperty("cp.version"));

  @Container
  private static final KsqlContainer sslKsqlContainer =
      new SslKsqlContainer(container, KSQLDB_TRUSTSTORE_JKS, KSQLDB_KEYSTORE_JKS);

  @Test
  public void testStreamTableCreateAndDelete() throws IOException {
    KsqlClientConfig config =
        KsqlClientConfig.builder()
            .setServer(sslKsqlContainer.getUrl())
            .setUseAlpn(true)
            .setTrustStore(getClass().getResource(KSQLDB_TRUSTSTORE_JKS).getPath())
            .setTrustStorePassword("ksqldb")
            .build();
    KsqlApiClient client = new KsqlApiClient(config);

    String streamName = "riderLocations";

    String sql =
        "CREATE STREAM "
            + streamName
            + " (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE)\n"
            + "  WITH (kafka_topic='locations', value_format='json', partitions=1);";

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
            + "     KEY_FORMAT='KAFKA', PARTITIONS=2, REPLICAS=1,"
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
