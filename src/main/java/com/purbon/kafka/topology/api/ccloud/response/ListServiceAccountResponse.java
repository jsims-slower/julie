package com.purbon.kafka.topology.api.ccloud.response;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ListServiceAccountResponse {

  private String api_version;
  private String kind;
  private CCloudMetadataListResponse metadata;
  private List<ServiceAccountResponse> data;
}
