package com.linkedin.venice.vpj.pubsub.input;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


public enum SplitType {
  /**
   * Divides a {@link PubSubTopicPartition} into splits such that the total number of
   * splits does not exceed a given maximum. The size of each split is calculated by
   * dividing the total record count in the partition by this maximum number.
   *
   * <p>For example, if the partition has 100 records and the maximum splits is 5,
   * each split will contain approximately 20 records.</p>
   */
  CAPPED_SPLIT_COUNT,

  /**
   * Divides a {@link PubSubTopicPartition} into multiple splits, each containing
   * a fixed target number of records.
   */
  FIXED_RECORD_COUNT,

  /**
   * Creates exactly one split for the entire {@link PubSubTopicPartition}.
   */
  WHOLE_PARTITION,

  /**
   * Divides a {@link PubSubTopicPartition} into time-based intervals.
   * The range between the timestamp of the first record and the timestamp of the last record
   * in the partition is broken into fixed time units (for example, 30 minutes).
   * The positions corresponding to these time boundaries are then used to create splits.
   */
  TIME_WINDOW,
}
