package com.purbon.kafka.topology.audit;

public interface Appender extends AutoCloseable {

  default void init() {
    // no-op
  }

  default void close() {
    // no-op
  }

  void log(String msg);
}
