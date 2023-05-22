package com.purbon.kafka.topology.api.ccloud.response;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ResourceMetadataResponse {

  private String self;
  private String resource_name;
  private String created_at;
  private String updated_at;
  private String deleted_at;
}
