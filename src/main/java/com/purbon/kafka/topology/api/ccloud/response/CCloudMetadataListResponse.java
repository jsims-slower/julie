package com.purbon.kafka.topology.api.ccloud.response;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CCloudMetadataListResponse {

  private String first;
  private String last;
  private String prev;
  private String next;
  private int total_size;
}
