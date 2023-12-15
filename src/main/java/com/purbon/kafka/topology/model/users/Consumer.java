package com.purbon.kafka.topology.model.users;

import com.purbon.kafka.topology.model.User;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Consumer extends User {

  private String group;

  public Consumer(String principal) {
    this(principal, null);
  }

  public Consumer(String principal, String group) {
    super(principal);
    this.group = group;
  }

  public String groupString() {
    return Optional.ofNullable(group).orElse("*");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Consumer)) {
      return false;
    }
    Consumer consumer = (Consumer) o;
    return getPrincipal().equals(consumer.getPrincipal())
        && groupString().equals(consumer.groupString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupString(), getPrincipal());
  }
}
