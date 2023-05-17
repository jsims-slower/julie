package com.purbon.kafka.topology.model.users;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class PrincipalTest {
  @Test
  public void readsUserPrincipal() {
    ConfluentCloudPrincipal principal = ConfluentCloudPrincipal.fromString("User:sa-foo");
    assertEquals("sa-foo", principal.getServiceAccountName());
    assertEquals(ConfluentCloudPrincipal.PrincipalType.User, principal.getPrincipalType());
  }

  @Test
  public void readsGroupPrincipal() {
    ConfluentCloudPrincipal principal = ConfluentCloudPrincipal.fromString("Group:sa-bar");
    assertEquals("sa-bar", principal.getServiceAccountName());
    assertEquals(ConfluentCloudPrincipal.PrincipalType.Group, principal.getPrincipalType());
  }

  @Test
  public void failsForMalformedPrincipalString() {
    assertThrows(IllegalArgumentException.class, () -> ConfluentCloudPrincipal.fromString(""));
  }

  @Test
  public void failsForInvalidPrincipalTypeString() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ConfluentCloudPrincipal.fromString("Vulcan:sa-spock"));
  }

  @Test
  public void roundTripFromUserPrincipalString() {
    var userPrincipalString = "User:sa-foo";
    assertEquals(
        userPrincipalString, ConfluentCloudPrincipal.fromString(userPrincipalString).toString());
  }

  @Test
  public void roundTripFromGroupPrincipalString() {
    var groupPrincipalString = "Group:sa-bar";
    assertEquals(
        groupPrincipalString, ConfluentCloudPrincipal.fromString(groupPrincipalString).toString());
  }

  @Test
  public void generatesMappedPrincipal() {
    var mappedPrincipal = "User:123456";
    ConfluentCloudPrincipal principal = ConfluentCloudPrincipal.fromString("User:sa-foo");
    assertEquals(mappedPrincipal, principal.toMappedPrincipalString(123456L));
  }
}
