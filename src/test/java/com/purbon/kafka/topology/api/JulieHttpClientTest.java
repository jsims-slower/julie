package com.purbon.kafka.topology.api;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.Constants.JULIE_HTTP_BACKOFF_TIME_MS;
import static com.purbon.kafka.topology.Constants.JULIE_HTTP_RETRY_TIMES;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.utils.PTHttpClient;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@WireMockTest(httpPort = 8089)
public class JulieHttpClientTest {

  private final Map<String, String> cliOps = Collections.singletonMap(BROKERS_OPTION, "");
  private final Properties props = new Properties();

  private PTHttpClient client;

  @BeforeEach
  public void before() throws IOException {
    props.put(JULIE_HTTP_BACKOFF_TIME_MS, 0);
  }

  @Test
  public void shouldResponseFastToNonRetryErrorCodes(WireMockRuntimeInfo wireMockRuntimeInfo)
      throws IOException {
    Configuration config = new Configuration(cliOps, props);

    client = new PTHttpClient(wireMockRuntimeInfo.getHttpBaseUrl(), config);

    stubFor(
        get(urlEqualTo("/some/thing"))
            .willReturn(
                aResponse().withHeader("Content-Type", "text/plain").withBody("Hello world!")));
    stubFor(get(urlEqualTo("/some/thing/else")).willReturn(aResponse().withStatus(404)));
    assertThat(client.doGet("/some/thing").getStatus()).isEqualTo(200);
    assertThat(client.doGet("/some/thing/else").getStatus()).isEqualTo(404);
  }

  @Test
  public void shouldRunTheRetryFlowForRetrievableErrorCodes(WireMockRuntimeInfo wireMockRuntimeInfo)
      throws IOException {

    props.put(JULIE_HTTP_RETRY_TIMES, 5);
    Configuration config = new Configuration(cliOps, props);

    client = new PTHttpClient(wireMockRuntimeInfo.getHttpBaseUrl(), config);

    stubFor(
        get(urlEqualTo("/some/thing"))
            .inScenario("retrievable")
            .whenScenarioStateIs(STARTED)
            .willReturn(aResponse().withStatus(429))
            .willSetStateTo("retry1"));

    stubFor(
        get(urlEqualTo("/some/thing"))
            .inScenario("retrievable")
            .whenScenarioStateIs("retry1")
            .willReturn(aResponse().withStatus(503))
            .willSetStateTo("retry2"));

    stubFor(
        get(urlEqualTo("/some/thing"))
            .inScenario("retrievable")
            .whenScenarioStateIs("retry2")
            .willReturn(
                aResponse().withHeader("Content-type", "text/plain").withBody("Hello world!")));

    assertThat(client.doGet("/some/thing").getStatus()).isEqualTo(200);
  }
}
