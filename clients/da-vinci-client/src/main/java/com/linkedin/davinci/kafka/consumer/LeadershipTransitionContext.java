package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.replica.state.DoLStamp;


public class LeadershipTransitionContext {
  private final long termId;

  /**
   * DoLStamp sent to announce leadership for this partition corresponding to the most recent term.
   *  DoLStamp: Declaration of Leadership Stamp
   */
  private DoLStamp doLStamp;

  /**
   * Indicates the stage of the leader transition process.
   */
  private LeaderTransitionStage leaderTransitionStage = LeaderTransitionStage.BEGIN;

  public LeadershipTransitionContext(long termId) {
    this.termId = termId;
  }

  public LeaderTransitionStage getLeaderTransitionStage() {
    return leaderTransitionStage;
  }

  public void setLeaderTransitionStage(LeaderTransitionStage leaderTransitionStage) {
    this.leaderTransitionStage = leaderTransitionStage;
  }

  public long getTermId() {
    return termId;
  }

  public DoLStamp getDoLStamp() {
    return doLStamp;
  }

  public void setDoLStamp(DoLStamp doLStamp) {
    this.doLStamp = doLStamp;
  }
}
