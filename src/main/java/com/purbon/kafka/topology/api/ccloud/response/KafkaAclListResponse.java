package com.purbon.kafka.topology.api.ccloud.response;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KafkaAclListResponse {

  private String kind;
  private KafkaAclListMetadata metadata;
  private List<KafkaAclResponse> data;
}
