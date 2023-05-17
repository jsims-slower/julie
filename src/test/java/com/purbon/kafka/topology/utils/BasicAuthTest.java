package com.purbon.kafka.topology.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class BasicAuthTest {

  @Test
  public void testToHttpAuthToken() {
    BasicAuth auth = new BasicAuth("user", "pass");
    assertThat(auth.toHttpAuthToken()).isEqualTo("Basic dXNlcjpwYXNz");
  }

  @Test
  public void testValuesRequired() {
    assertThatThrownBy(() -> new BasicAuth(null, "pass")).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new BasicAuth("user", null)).isInstanceOf(NullPointerException.class);
  }
}
