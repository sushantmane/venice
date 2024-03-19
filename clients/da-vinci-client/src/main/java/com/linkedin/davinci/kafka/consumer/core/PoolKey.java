package com.linkedin.davinci.kafka.consumer.core;

public enum PoolKey {
  CURRENT_LEADER_HYBRID_AA_WC(1), CURRENT_LEADER_HYBRID_AND_CURRENT_FOLLOWER(2), OTHER_LEADER_HYBRID_AA_WC(3),
  OTHER_LEADER_HYBRID_AND_OTHER_FOLLOWER(4);

  private final int priority;

  PoolKey(int priority) {
    this.priority = priority;
  }

  public int getPriority() {
    return priority;
  }

  // returns a pointer to an appropriate pool based on the inputs
  public static PoolKey getPoolKey(VersionStatus versionStatus, ReplicaRole role, Workload workload) {
    if (versionStatus == VersionStatus.CURRENT && role == ReplicaRole.LEADER && (workload == Workload.HYBRID_AA_AND_WC
        || workload == Workload.HYBRID_WC || workload == Workload.HYBRID_AA)) {
      return CURRENT_LEADER_HYBRID_AA_WC;
    }
    if (versionStatus == VersionStatus.CURRENT) {
      return CURRENT_LEADER_HYBRID_AND_CURRENT_FOLLOWER;
    }
    if (role == ReplicaRole.LEADER && (workload == Workload.HYBRID_AA_AND_WC || workload == Workload.HYBRID_WC
        || workload == Workload.HYBRID_AA)) {
      return OTHER_LEADER_HYBRID_AA_WC;
    }
    return OTHER_LEADER_HYBRID_AND_OTHER_FOLLOWER;
  }
}
