package com.purbon.kafka.topology.audit;

import static com.purbon.kafka.topology.Constants.AUDIT_APPENDER_KAFKA_PREFIX;

import com.purbon.kafka.topology.Configuration;

import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaAppender implements Appender {

  private final Configuration config;
  private final Properties props;
  private KafkaProducer<String, String> producer;

  public KafkaAppender(Configuration config) {
    this.config = config;
    this.props = config.asProperties(AUDIT_APPENDER_KAFKA_PREFIX);
  }

  @Override
  public void init() {
    producer = new KafkaProducer<>(props);
  }

  @Override
  public void close() {
    Optional.ofNullable(producer).ifPresent(KafkaProducer::close);
  }

  @Override
  public void log(String msg) {
    var record = new ProducerRecord<String, String>(config.getKafkaAuditTopic(), msg);
    producer.send(record);
  }
}
