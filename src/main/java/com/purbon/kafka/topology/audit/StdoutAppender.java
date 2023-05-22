package com.purbon.kafka.topology.audit;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StdoutAppender implements Appender {

  @Override
  public void log(String msg) {
    try {
      flush(msg, System.out);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
  }

  protected void flush(String msg, OutputStream os) throws IOException {
    os.write(msg.getBytes(StandardCharsets.UTF_8));
  }
}
