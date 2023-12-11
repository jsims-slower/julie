package com.purbon.kafka.topology.backend;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.backend.kafka.KafkaBackendConsumer;
import com.purbon.kafka.topology.backend.kafka.KafkaBackendProducer;
import com.purbon.kafka.topology.backend.kafka.RecordReceivedCallback;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.WakeupException;

@Slf4j
public class KafkaBackend implements Backend, RecordReceivedCallback {

  private boolean isCompleted;

  private KafkaBackendConsumer consumer;
  private KafkaBackendProducer producer;

  private AtomicReference<BackendState> latest;
  private final AtomicBoolean shouldWaitForLoad = new AtomicBoolean(true);
  private String instanceId;
  private Thread thread;

  @RequiredArgsConstructor
  private static class JulieKafkaConsumerThread implements Runnable {
    private final KafkaBackend callback;
    private final KafkaBackendConsumer consumer;

    public void run() {
      consumer.start();
      try {
        consumer.retrieve(callback);
      } catch (WakeupException ex) {
        log.trace(ex.getMessage(), ex);
      }
    }
  }

  @SneakyThrows
  @Override
  public void configure(Configuration config) {
    instanceId = config.getJulieInstanceId();
    latest = new AtomicReference<>(new BackendState());
    shouldWaitForLoad.set(true);
    consumer = new KafkaBackendConsumer(config);
    consumer.configure();

    var topics = consumer.listTopics();
    if (!topics.containsKey(config.getJulieKafkaConfigTopic())) {
      throw new IOException(
          "The internal julie kafka configuration topic topic "
              + config.getJulieKafkaConfigTopic()
              + " should exist in the cluster");
    }
    producer = new KafkaBackendProducer(config);
    producer.configure();

    thread = new Thread(new JulieKafkaConsumerThread(this, consumer), "kafkaJulieConsumer");
    thread.start();
    waitForCompletion();
  }

  public synchronized void waitForCompletion() throws InterruptedException {
    while (!isCompleted) {
      wait(30000);
    }
  }

  public synchronized void complete() {
    isCompleted = true;
    notify();
  }

  @Override
  public void save(BackendState state) throws IOException {
    producer.save(state);
  }

  @SneakyThrows
  @Override
  public BackendState load() throws IOException {
    while (shouldWaitForLoad.get()) {
      continue;
    }
    return latest == null ? new BackendState() : latest.get();
  }

  public void initialLoadFinish() {
    shouldWaitForLoad.set(false);
  }

  @Override
  public void close() {
    consumer.stop();
    producer.stop();
    try {
      thread.join();
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    }
    latest = null;
    thread = null;
  }

  @Override
  public void apply(ConsumerRecord<String, BackendState> record) {
    if (instanceId.equals(record.key()) && latest != null) {
      latest.set(record.value());
    }
  }
}
