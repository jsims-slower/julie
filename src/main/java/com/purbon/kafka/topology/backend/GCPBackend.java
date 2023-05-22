package com.purbon.kafka.topology.backend;

import static com.purbon.kafka.topology.BackendController.STATE_FILE_NAME;

import com.google.cloud.storage.*;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.utils.JSON;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GCPBackend implements Backend {

  private Storage storage;
  private Configuration config;

  @Override
  public void configure(Configuration config) {
    configure(config, null);
  }

  public void configure(Configuration config, URI endpoint) {
    this.config = config;
    this.storage =
        StorageOptions.newBuilder().setProjectId(config.getGCPProjectId()).build().getService();
  }

  @Override
  public void save(BackendState state) throws IOException {
    BlobId blobId = BlobId.of(config.getGCPBucket(), STATE_FILE_NAME);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    try {
      storage.create(
          blobInfo,
          state.asJson().getBytes(StandardCharsets.UTF_8),
          Storage.BlobTargetOption.detectContentType());
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new IOException(ex);
    }
  }

  @Override
  public BackendState load() throws IOException {
    try {
      Blob blob = storage.get(BlobId.of(config.getGCPBucket(), STATE_FILE_NAME));
      String contentJson = new String(blob.getContent(), StandardCharsets.UTF_8);
      return JSON.toObject(contentJson, BackendState.class);
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new IOException(ex);
    }
  }

  @Override
  public void close() {
    // empty
  }
}
