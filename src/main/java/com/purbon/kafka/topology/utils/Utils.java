package com.purbon.kafka.topology.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {

  public static Stream<String> asNullableStream(List<String> items) {
    return Stream.ofNullable(items).flatMap(Collection::stream);
  }

  public static Path filePath(String file, String rootPath) {
    Path mayBeAbsolutePath = Paths.get(file);
    Path path = mayBeAbsolutePath.isAbsolute() ? mayBeAbsolutePath : Paths.get(rootPath, file);
    log.debug("Artefact File {} loaded from {}", file, path);
    return path;
  }
}
