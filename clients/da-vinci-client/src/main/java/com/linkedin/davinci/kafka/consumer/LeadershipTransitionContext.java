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
  private final LeaderTransitionStage leaderTransitionStage;

  public LeadershipTransitionContext(LeaderTransitionStage leaderTransitionStage, long termId) {
    this.leaderTransitionStage = leaderTransitionStage;
    this.termId = termId;
  }

  public LeaderTransitionStage getLeaderTransitionStage() {
    return leaderTransitionStage;
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
