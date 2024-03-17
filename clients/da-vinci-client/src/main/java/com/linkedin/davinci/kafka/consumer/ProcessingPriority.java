package com.linkedin.davinci.kafka.consumer;

public enum ProcessingPriority {
  FOLLOWER(1), LEADER_NO_WC_NO_AA(2), LEADER_WC_NO_AA(3), LEADER_NO_WC_AA(4), LEADER_WC_AA(5);

  private final int priorityValue;

  ProcessingPriority(int priorityValue) {
    this.priorityValue = priorityValue;
  }

  public int getPriorityValue() {
    return priorityValue;
  }

  public static ProcessingPriority fromPriorityValue(int priorityValue) {
    for (ProcessingPriority priority: values()) {
      if (priority.getPriorityValue() == priorityValue) {
        return priority;
      }
    }
    throw new IllegalArgumentException("Invalid priority value: " + priorityValue);
  }

  /**
   * Compute the processing priority based on the leader status and the enabled status of watermark and auto-ack.
   * @param isLeader whether the consumer is the leader
   * @param isWcEnabled whether write-compute is enabled
   * @param isAaEnabled whether active-active replication is enabled
   * @return the processing priority
   */
  static int computeProcessingPriority(boolean isLeader, boolean isWcEnabled, boolean isAaEnabled) {
    if (!isLeader) {
      return ProcessingPriority.FOLLOWER.getPriorityValue();
    }
    if (!isWcEnabled && !isAaEnabled) {
      return ProcessingPriority.LEADER_NO_WC_NO_AA.getPriorityValue();
    }
    if (isWcEnabled && !isAaEnabled) {
      return ProcessingPriority.LEADER_WC_NO_AA.getPriorityValue();
    }
    if (!isWcEnabled) {
      return ProcessingPriority.LEADER_NO_WC_AA.getPriorityValue();
    }
    return ProcessingPriority.LEADER_WC_AA.getPriorityValue();
  }
}
