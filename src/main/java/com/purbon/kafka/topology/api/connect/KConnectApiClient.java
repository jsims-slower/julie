package com.purbon.kafka.topology.api.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.api.mds.Response;
import com.purbon.kafka.topology.clients.ArtefactClient;
import com.purbon.kafka.topology.clients.JulieHttpClient;
import com.purbon.kafka.topology.model.Artefact;
import com.purbon.kafka.topology.model.artefact.KafkaConnectArtefact;
import com.purbon.kafka.topology.utils.BasicAuth;
import com.purbon.kafka.topology.utils.JSON;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KConnectApiClient extends JulieHttpClient implements ArtefactClient {

  public KConnectApiClient(String server, Configuration config) throws IOException {
    this(server, "", config);
  }

  private final String label;

  public KConnectApiClient(String server, String label, Configuration config) throws IOException {
    super(server, Optional.of(config));
    this.label = label;
    // configure basic authentication if available
    Map<String, String> basicAuths = config.getServersBasicAuthMap();
    if (basicAuths.containsKey(label)) {
      String[] values = basicAuths.get(label).split(":");
      setBasicAuth(new BasicAuth(values[0], values[1]));
    }
  }

  @Override
  public Map<String, Object> add(String content) throws IOException {
    throw new IOException("Not implemented in this context");
  }

  public List<String> list() throws IOException {
    JsonNode node = doList();
    return ImmutableList.copyOf(node.fieldNames());
  }

  protected JsonNode doList() throws IOException {
    Response response = doGet("/connectors?expand=info");
    return JSON.toNode(response.getResponseAsString());
  }

  @Override
  public Collection<? extends Artefact> getClusterState() throws IOException {
    JsonNode list = doList();
    Iterable<Map.Entry<String, JsonNode>> fields = list::fields;
    return StreamSupport.stream(fields.spliterator(), false)
        .map(
            entry -> {
              JsonNode config =
                  Optional.ofNullable(entry.getValue().get("info"))
                      .map(i -> i.get("config"))
                      .orElse(null);
              String hash = null;
              if (config instanceof ObjectNode) {
                ObjectNode node = (ObjectNode) config;
                node.remove("name");
                hash = Integer.toHexString(node.hashCode());
              }
              return new KafkaConnectArtefact("", server, entry.getKey(), hash);
            })
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, Object> add(String name, String config) throws IOException {
    String url = String.format("/connectors/%s/config", name);

    Map<String, Object> map = JSON.toMap(config);
    if (mayBeAConfigRecord(map)) {
      var content = map.get("config");
      if (!name.equalsIgnoreCase(map.get("name").toString())) {
        throw new IOException("Trying to add a connector with a different name as in the topology");
      }
      config = JSON.asString(content);
    }

    String response = doPut(url, config);
    return JSON.toMap(response);
  }

  private boolean mayBeAConfigRecord(Map<String, Object> map) {
    Set<String> keySet = map.keySet();
    return keySet.contains("config") && keySet.contains("name") && keySet.size() == 2;
  }

  public void delete(String connector) throws IOException {
    doDelete("/connectors/" + connector + "/", "");
  }

  public String status(String connectorName) throws IOException {
    Response response = doGet("/connectors/" + connectorName + "/status");
    Map<String, Map<String, String>> map = JSON.toMap(response.getResponseAsString());

    if (map.containsKey("error_code")) {
      throw new IOException(map.get("message").toString());
    }

    return map.get("connector").get("state");
  }

  public void pause(String connectorName) throws IOException {
    doPut("/connectors/" + connectorName + "/pause");
  }

  @Override
  public Map<String, Object> update(String name, String config) throws IOException {
    return add(name, config);
  }

  @Override
  public String toString() {
    return "KConnectApiClient{" + server + " - " + label + "}";
  }
}
