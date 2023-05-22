package com.purbon.kafka.topology;

import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface AccessControlProvider {

  void createBindings(Collection<TopologyAclBinding> bindings) throws IOException;

  void clearBindings(Collection<TopologyAclBinding> bindings) throws IOException;

  Map<String, List<TopologyAclBinding>> listAcls();
}
