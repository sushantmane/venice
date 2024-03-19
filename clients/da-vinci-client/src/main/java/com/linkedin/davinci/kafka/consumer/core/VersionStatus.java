package com.linkedin.davinci.kafka.consumer.core;

public enum VersionStatus {
  CURRENT(1), BACKUP(2), FUTURE(3);

  private final int value;

  VersionStatus(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static VersionStatus fromValue(int value) {
    for (VersionStatus status: VersionStatus.values()) {
      if (status.getValue() == value) {
        return status;
      }
    }
    throw new IllegalArgumentException("Invalid value: " + value);
  }
}
