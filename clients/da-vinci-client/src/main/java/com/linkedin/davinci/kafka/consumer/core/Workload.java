package com.linkedin.davinci.kafka.consumer.core;

public enum Workload {
  HYBRID_AA_AND_WC(1), HYBRID_AA(2), HYBRID_WC(3), HYBRID(4), // no reads before record processing
  LIGHT(5); // no reads before record processing

  private final int value;

  Workload(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static Workload fromValue(int value) {
    for (Workload workload: Workload.values()) {
      if (workload.getValue() == value) {
        return workload;
      }
    }
    throw new IllegalArgumentException("Invalid value: " + value);
  }
}
