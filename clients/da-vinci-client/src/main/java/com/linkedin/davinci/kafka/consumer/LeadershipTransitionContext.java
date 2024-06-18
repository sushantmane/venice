package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;


public class LeadershipTransitionContext {
  private final long termId;

  /**
   * Future representing the DoLStamp message sent to announce leadership for this partition corresponding to the most recent term.
   *  DoLStamp: Declaration of Leadership Stamp
   */
  private PubSubProduceResult latestDoLStamp;

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

  public PubSubProduceResult getLatestDoLStamp() {
    return latestDoLStamp;
  }

  public void setLatestDoLStamp(PubSubProduceResult latestDoLStamp) {
    this.latestDoLStamp = latestDoLStamp;
  }
}
