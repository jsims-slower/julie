package com.purbon.kafka.topology.utils;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class ZKClient {

  private final ZKConnection connection;
  private ZooKeeper zkClient;

  public ZKClient() {
    connection = new ZKConnection();
  }

  public void connect(String host) throws IOException, InterruptedException {
    zkClient = connection.connect(host);
  }

  public String getNodeData(String path) throws IOException {
    try {
      byte[] data = zkClient.getData(path, null, null);
      return new String(data);
    } catch (Exception e) {
      log.error("{}", e.getMessage(), e);
      throw new IOException(e);
    }
  }
}
