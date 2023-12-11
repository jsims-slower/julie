package com.purbon.kafka.topology.model.artefact;

import com.purbon.kafka.topology.model.Artefacts;
import java.util.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@AllArgsConstructor
public class KsqlArtefacts implements Artefacts {

  @Setter private List<KsqlStreamArtefact> streams;
  @Setter private List<KsqlTableArtefact> tables;
  private final KsqlVarsArtefact vars;

  public KsqlArtefacts() {
    this(new ArrayList<>(), new ArrayList<>(), new KsqlVarsArtefact(Collections.emptyMap()));
  }
}
