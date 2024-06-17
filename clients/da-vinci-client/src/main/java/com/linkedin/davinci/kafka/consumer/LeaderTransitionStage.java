package com.linkedin.davinci.kafka.consumer;

public enum LeaderTransitionStage {
  BEGIN(0), LOOPBACK_INITIATED(1), LOOPBACK_COMPLETED(2), LEADER_INITIATED(3), LEADER_COMPLETED(4), END(5);

  private final int index;

  LeaderTransitionStage(int index) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }
}
