package com.purbon.kafka.topology.model;

import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class PlatformSystem<T extends User> {
  private final List<T> accessControlLists;
  private final Artefacts artefacts;

  public PlatformSystem() {
    this(Collections.emptyList(), null);
  }

  public PlatformSystem(List<T> accessControlLists) {
    this(accessControlLists, null);
  }
}
