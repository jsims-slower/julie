package com.purbon.kafka.topology;

import com.purbon.kafka.topology.api.connect.KConnectApiClient;
import com.purbon.kafka.topology.api.ksql.KsqlApiClient;
import com.purbon.kafka.topology.audit.*;
import com.purbon.kafka.topology.backend.*;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

@Slf4j
public class JulieOpsAuxiliary {

  public static BackendController buildBackendController(Configuration config) throws IOException {
    String backendClass = config.getStateProcessorImplementationClassName();
    Backend backend = initializeClassFromString(backendClass, config);
    backend.configure(config);
    return new BackendController(backend);
  }

  public static Auditor configureAndBuildAuditor(Configuration config) throws IOException {
    if (!config.isJulieAuditEnabled()) {
      return new VoidAuditor();
    }
    String appenderClassString = config.getJulieAuditAppenderClass();
    Appender appender = initializeClassFromString(appenderClassString, config);
    return new Auditor(appender);
  }

  private static <T> T initializeClassFromString(String classNameString, Configuration config)
      throws IOException {
    try {
      @SuppressWarnings("unchecked")
      Class<T> aClass = (Class<T>) Class.forName(classNameString);
      try {
        return aClass.getConstructor(Configuration.class).newInstance(config);
      } catch (NoSuchMethodException e) {
        log.trace("{} has no config constructor, falling back to a default one", classNameString);
        return aClass.getConstructor().newInstance();
      }
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InstantiationException
        | InvocationTargetException e) {
      throw new IOException(e);
    }
  }

  public static KafkaConnectArtefactManager configureKConnectArtefactManager(
      Configuration config, String topologyFileOrDir) throws IOException {
    Map<String, KConnectApiClient> clients = new HashMap<>();
    for (var entry : config.getKafkaConnectServers().entrySet()) {
      var value = new KConnectApiClient(entry.getValue(), entry.getKey(), config);
      if (clients.put(entry.getKey(), value) != null) {
        throw new IllegalStateException("Duplicate key");
      }
    }

    if (clients.isEmpty()) {
      log.debug(
          "No KafkaConnect clients configured for JulieOps to use, please verify your config file");
    }

    return new KafkaConnectArtefactManager(clients, config, topologyFileOrDir);
  }

  public static KSqlArtefactManager configureKSqlArtefactManager(
      Configuration config, String topologyFileOrDir) {

    Map<String, KsqlApiClient> clients = new HashMap<>();
    if (config.hasKSQLServer()) {
      KsqlApiClient client = new KsqlApiClient(config.getKSQLClientConfig());
      clients.put("default", client);
    }

    if (clients.isEmpty()) {
      log.debug("No KSQL clients configured for JulieOps to use, please verify your config file");
    }

    return new KSqlArtefactManager(clients, config, topologyFileOrDir);
  }

  public static void configureLogsInDebugMode(Configuration config) {
    if (!config.areJulieLogsInDebugMode()) {
      return;
    }
    Configurator.setAllLevels("com.purbon.kafka", Level.DEBUG);
  }
}
