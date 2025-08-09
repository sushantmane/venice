package com.linkedin.venice.vpj.pubsub.input;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


/**
 * Describes a contiguous range of records within a single {@link PubSubTopicPartition}.
 * The range is bounded by a start position (inclusive) and an end position (exclusive).
 * It is used to enable parallel reading during repush jobs, where data is read from
 * pub-sub topics and written into a new version of a Venice store.
 *
 * <p><b>Scope and constraints:</b>
 * <ul>
 *   <li>One {@code PubSubPartitionRangeSplit} covers exactly one topic partition.
 *       It never spans multiple partitions.</li>
 *   <li>{@code startPubSubPosition} must be less than or equal to
 *       {@code endPubSubPosition} according to the position comparator.</li>
 *   <li>Instances should be treated as immutable once constructed.</li>
 * </ul>
 *
 * <p><b>Engine independence:</b>
 * This is a framework-agnostic descriptor. Adapters can translate it to a
 * Hadoop {@code InputSplit} or a Spark DataSource V2 {@code InputPartition}.
 */
public class PubSubPartitionRangeSplit {
  /** The topic partition this range belongs to. */
  private final PubSubTopicPartition pubSubTopicPartition;

  /** Inclusive; the first record is read from this position. */
  private final PubSubPosition startPubSubPosition;

  /** Exclusive; reading stops before this position. */
  private final PubSubPosition endPubSubPosition;

  /** Total records in the range, usually derived from the difference between start and end positions. */
  private final long numberOfRecords;

  /** Monotonically increasing index for ranges within the same topic partition, starting from 0. */
  private final int rangeIndex;

  public PubSubPartitionRangeSplit(
      PubSubTopicPartition pubSubTopicPartition,
      PubSubPosition startPubSubPosition,
      PubSubPosition endPubSubPosition,
      long numberOfRecords,
      int rangeIndex) {
    this.pubSubTopicPartition = pubSubTopicPartition;
    this.startPubSubPosition = startPubSubPosition;
    this.endPubSubPosition = endPubSubPosition;
    this.numberOfRecords = numberOfRecords;
    this.rangeIndex = rangeIndex;
  }

  public PubSubTopicPartition getPubSubTopicPartition() {
    return pubSubTopicPartition;
  }

  public PubSubPosition getStartPubSubPosition() {
    return startPubSubPosition;
  }

  public PubSubPosition getEndPubSubPosition() {
    return endPubSubPosition;
  }

  public long getNumberOfRecords() {
    return numberOfRecords;
  }

  public int getRangeIndex() {
    return rangeIndex;
  }
}
