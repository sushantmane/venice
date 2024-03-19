package com.linkedin.davinci.kafka.consumer.core;

public enum ReplicaRole {
  LEADER(1), FOLLOWER(2);

  private final int value;

  ReplicaRole(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static ReplicaRole fromValue(int value) {
    for (ReplicaRole role: ReplicaRole.values()) {
      if (role.getValue() == value) {
        return role;
      }
    }
    throw new IllegalArgumentException("Invalid value for ReplicaRole: " + value);
  }
}
