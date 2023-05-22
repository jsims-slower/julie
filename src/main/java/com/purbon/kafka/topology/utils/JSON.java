package com.purbon.kafka.topology.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.util.List;
import java.util.Map;

public class JSON {

  private static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.registerModule(new Jdk8Module());
    mapper.findAndRegisterModules();
  }

  public static <T> Map<String, T> toMap(String jsonString) throws JsonProcessingException {
    return mapper.readValue(jsonString, new TypeReference<>() {});
  }

  public static String asString(Map<?, ?> map) throws JsonProcessingException {
    return mapper.writeValueAsString(map);
  }

  public static String asPrettyString(Map<?, ?> map) throws JsonProcessingException {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
  }

  public static List<String> toArray(String jsonString) throws JsonProcessingException {
    return mapper.readValue(jsonString, new TypeReference<>() {});
  }

  public static String asString(Object object) throws JsonProcessingException {
    return mapper.writeValueAsString(object);
  }

  public static String asPrettyString(Object object) throws JsonProcessingException {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
  }

  public static <T> List<T> toObjectList(String jsonString, Class<T> objectClazz)
      throws JsonProcessingException {
    CollectionType collectionType =
        mapper.getTypeFactory().constructCollectionType(List.class, objectClazz);
    return mapper.readValue(jsonString, collectionType);
  }

  public static <T> T toObject(String jsonString, Class<T> objectClazz)
      throws JsonProcessingException {
    return mapper.readValue(jsonString, objectClazz);
  }

  public static JsonNode toNode(String jsonString) throws JsonProcessingException {
    return mapper.readTree(jsonString);
  }
}
