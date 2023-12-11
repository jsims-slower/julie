package com.purbon.kafka.topology.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.purbon.kafka.topology.utils.JSON;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public abstract class BaseAction implements Action {

  protected abstract Map<String, Object> props();

  protected abstract Collection<Map<String, Object>> detailedProps();

  @Override
  public List<String> refs() {
    return detailedProps().stream()
        .map(
            map -> {
              try {
                return JSON.asPrettyString(map);
              } catch (JsonProcessingException ex) {
                // TODO: Swallowing exceptions is dangerous
                return "";
              }
            })
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    try {
      final Map<String, Object> props = props();
      return props.isEmpty() ? "" : JSON.asPrettyString(props);
    } catch (JsonProcessingException e) {
      // TODO: Swallowing exceptions is dangerous
      return "";
    }
  }

  protected static <T> Map<String, T> sortMap(Map<String, T> unsortedMap) {
    TreeMap<String, T> treeMap = new TreeMap<>(Comparator.naturalOrder());
    treeMap.putAll(unsortedMap);
    return treeMap;
  }

  protected static <T, K, U> Collector<T, ?, TreeMap<K, U>> toSortedMap(
      Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper) {
    return Collectors.toMap(
        keyMapper,
        valueMapper,
        (v1, v2) -> {
          throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));
        },
        TreeMap::new);
  }
}
