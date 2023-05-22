package com.purbon.kafka.topology.audit;

import com.purbon.kafka.topology.actions.Action;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Auditor implements AutoCloseable {

  @Getter private final Appender appender;

  public void log(Action action) {
    action.refs().forEach(appender::log);
  }

  @Override
  public void close() {
    appender.close();
  }
}
