package com.purbon.kafka.topology.actions;

import java.io.IOException;
import java.util.Collection;

public interface Action {

  void run() throws IOException;

  Collection<String> refs();
}
