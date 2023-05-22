package com.purbon.kafka.topology.api.ccloud;

import static com.purbon.kafka.topology.Constants.MANAGED_BY;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.api.ccloud.requests.KafkaAclRequest;
import com.purbon.kafka.topology.api.ccloud.requests.ServiceAccountRequest;
import com.purbon.kafka.topology.api.ccloud.response.KafkaAclListResponse;
import com.purbon.kafka.topology.api.ccloud.response.ListServiceAccountResponse;
import com.purbon.kafka.topology.api.ccloud.response.ServiceAccountResponse;
import com.purbon.kafka.topology.api.ccloud.response.ServiceAccountV1Response;
import com.purbon.kafka.topology.api.mds.Response;
import com.purbon.kafka.topology.clients.JulieHttpClient;
import com.purbon.kafka.topology.model.cluster.ServiceAccount;
import com.purbon.kafka.topology.model.cluster.ServiceAccountV1;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import com.purbon.kafka.topology.utils.JSON;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CCloudApi {

  static final String V1_IAM_SERVICE_ACCOUNTS_URL = "/service_accounts";
  static final String V2_IAM_SERVICE_ACCOUNTS_URL = "/iam/v2/service-accounts";
  static final String V3_KAFKA_CLUSTER_URL = "/kafka/v3/clusters/";

  private final JulieHttpClient clusterHttpClient;
  private final JulieHttpClient ccloudApiHttpClient;

  private static final String ccloudApiBaseUrl = "https://api.confluent.cloud";
  private static final String V3_KAFKA_CLUSTER_ACL_PATTERN = V3_KAFKA_CLUSTER_URL + "%s/acls";

  private final int serviceAccountPageSize;

  public CCloudApi(String baseServerUrl, Configuration config) throws IOException {
    this(
        new JulieHttpClient(baseServerUrl, Optional.of(config)),
        new JulieHttpClient(ccloudApiBaseUrl, Optional.of(config)),
        config);
  }

  public CCloudApi(
      JulieHttpClient clusterHttpClient,
      JulieHttpClient ccloudApiHttpClient,
      Configuration config) {
    this.clusterHttpClient = clusterHttpClient;
    this.ccloudApiHttpClient = ccloudApiHttpClient;
    this.clusterHttpClient.setBasicAuth(config.getConfluentCloudClusterAuth());
    this.ccloudApiHttpClient.setBasicAuth(config.getConfluentCloudCloudApiAuth());

    this.serviceAccountPageSize = config.getConfluentCloudServiceAccountQuerySize();
  }

  public void createAcl(String clusterId, TopologyAclBinding binding) throws IOException {
    String url = String.format(V3_KAFKA_CLUSTER_ACL_PATTERN, clusterId);
    var request =
        new KafkaAclRequest(binding, String.format("%s%s", clusterHttpClient.getServer(), url));
    clusterHttpClient.doPost(url, JSON.asString(request));
  }

  public void deleteAcls(String clusterId, TopologyAclBinding binding) throws IOException {
    String url = String.format(V3_KAFKA_CLUSTER_ACL_PATTERN, clusterId);
    KafkaAclRequest request = new KafkaAclRequest(binding, url);
    clusterHttpClient.doDelete(request.deleteUrl());
  }

  public List<TopologyAclBinding> listAcls(String clusterId) throws IOException {
    String url = String.format(V3_KAFKA_CLUSTER_ACL_PATTERN, clusterId);
    List<TopologyAclBinding> acls = new ArrayList<>();
    do {
      Response rawResponse = clusterHttpClient.doGet(url);
      KafkaAclListResponse response =
          JSON.toObject(rawResponse.getResponseAsString(), KafkaAclListResponse.class);
      acls.addAll(
          response.getData().stream().map(TopologyAclBinding::new).collect(Collectors.toList()));
      url = response.getMetadata().getNext();
    } while (url != null);
    return acls;
  }

  public ServiceAccount createServiceAccount(String sa) throws IOException {
    return createServiceAccount(sa, MANAGED_BY);
  }

  public void deleteServiceAccount(String sa) throws IOException {
    String url = String.format("%s/%s", V2_IAM_SERVICE_ACCOUNTS_URL, sa);
    ccloudApiHttpClient.doDelete(url);
  }

  public ServiceAccount createServiceAccount(String sa, String description) throws IOException {
    var request = new ServiceAccountRequest(sa, description);
    var requestJson = JSON.asString(request);
    log.debug("createServiceAccount request={}", requestJson);
    String responseBody = ccloudApiHttpClient.doPost(V2_IAM_SERVICE_ACCOUNTS_URL, requestJson);

    ServiceAccountResponse response = JSON.toObject(responseBody, ServiceAccountResponse.class);
    return new ServiceAccount(
        response.getId(),
        response.getDisplay_name(),
        response.getDescription(),
        response.getMetadata().getResource_name());
  }

  public Set<ServiceAccount> listServiceAccounts() throws IOException {
    String url = V2_IAM_SERVICE_ACCOUNTS_URL;
    boolean finished;
    Set<ServiceAccount> accounts = new HashSet<>();

    do {
      ListServiceAccountResponse response = getListServiceAccounts(url, serviceAccountPageSize);
      for (ServiceAccountResponse serviceAccountResponse : response.getData()) {
        var resourceId = serviceAccountResponse.getMetadata().getResource_name();
        var serviceAccount =
            new ServiceAccount(
                serviceAccountResponse.getId(),
                serviceAccountResponse.getDisplay_name(),
                serviceAccountResponse.getDescription(),
                resourceId);
        accounts.add(serviceAccount);
      }

      String nextUrl = response.getMetadata().getNext();
      finished = nextUrl == null;
      if (!finished) {
        url = nextUrl.replace(ccloudApiBaseUrl, "");
      }
    } while (!finished);

    return accounts;
  }

  public Set<ServiceAccountV1> listServiceAccountsV1() throws IOException {
    Set<ServiceAccountV1> accounts = new HashSet<>();
    ServiceAccountV1Response response = getServiceAccountsV1(V1_IAM_SERVICE_ACCOUNTS_URL);
    if (response.getError() == null) {
      accounts = new HashSet<>(response.getUsers());
    }
    return accounts;
  }

  private ServiceAccountV1Response getServiceAccountsV1(String url) throws IOException {
    Response r = ccloudApiHttpClient.doGet(url);
    return JSON.toObject(r.getResponseAsString(), ServiceAccountV1Response.class);
  }

  private ListServiceAccountResponse getListServiceAccounts(String url, int page_size)
      throws IOException {
    String requestUrl = url;
    if (!url.contains("page_token")) {
      requestUrl = String.format("%s?page_size=%d", url, page_size);
    }
    Response r = ccloudApiHttpClient.doGet(requestUrl);
    return JSON.toObject(r.getResponseAsString(), ListServiceAccountResponse.class);
  }
}
