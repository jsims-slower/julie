package com.purbon.kafka.topology.api.ccloud.response;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ServiceAccountResponse {

  private String api_version;
  private String kind;
  private String id;
  private ResourceMetadataResponse metadata;
  private String display_name;
  private String description;
}
