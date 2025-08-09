package com.linkedin.venice.vpj.pubsub.input;

import static com.linkedin.venice.hadoop.input.kafka.KafkaInputFormat.DEFAULT_KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import java.time.Duration;
import java.util.Objects;


public final class SplitRequest {
  private static final long DEFAULT_RECORDS_PER_SPLIT = DEFAULT_KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
  private static final long DEFAULT_TIME_WINDOW_MS = Duration.ofHours(3).toMillis();
  private static final int DEFAULT_MAX_SPLITS = 5;

  private final PubSubTopicPartition pubSubTopicPartition;
  private final TopicManager topicManager;
  private final SplitType splitType;

  // Optionals (boxed so null means unset)
  private final Integer maxSplits; // for CAPPED_SPLIT_COUNT
  private final Long recordsPerSplit; // for FIXED_RECORD_COUNT
  private final Long timeWindowInMs; // for TIME_WINDOW

  private SplitRequest(Builder builder) {
    this.pubSubTopicPartition = builder.pubSubTopicPartition;
    this.topicManager = builder.topicManager;
    this.splitType = builder.splitType;
    this.maxSplits = builder.maxSplits;
    this.recordsPerSplit = builder.recordsPerSplit;
    this.timeWindowInMs = builder.timeWindowInMs;
  }

  public PubSubTopicPartition getPubSubTopicPartition() {
    return pubSubTopicPartition;
  }

  public TopicManager getTopicManager() {
    return topicManager;
  }

  public SplitType getSplitType() {
    return splitType;
  }

  public Integer getMaxSplits() {
    return maxSplits;
  }

  public long getRecordsPerSplit() {
    return recordsPerSplit;
  }

  public long getTimeWindowInMs() {
    return timeWindowInMs;
  }

  @Override
  public String toString() {
    return "SplitRequest{" + "pubSubTopicPartition=" + pubSubTopicPartition + ", splitType=" + splitType
        + ", maxSplits=" + maxSplits + ", recordsPerSplit=" + recordsPerSplit + ", timeWindowInMs=" + timeWindowInMs
        + '}';
  }

  public static final class Builder {
    private PubSubTopicPartition pubSubTopicPartition;
    private TopicManager topicManager;
    private SplitType splitType;

    private Integer maxSplits;
    private Long recordsPerSplit;
    private Long timeWindowInMs;

    public Builder() {
    }

    public Builder pubSubTopicPartition(PubSubTopicPartition value) {
      this.pubSubTopicPartition = value;
      return this;
    }

    public Builder topicManager(TopicManager value) {
      this.topicManager = value;
      return this;
    }

    public Builder splitType(SplitType value) {
      this.splitType = value;
      return this;
    }

    /** Used when splitType is CAPPED_SPLIT_COUNT. */
    public Builder maxSplits(Integer value) {
      this.maxSplits = value;
      return this;
    }

    /** Used when splitType is FIXED_RECORD_COUNT. */
    public Builder recordsPerSplit(long value) {
      this.recordsPerSplit = value;
      return this;
    }

    /** Used when splitType is TIME_WINDOW. */
    public Builder timeWindowInMs(long value) {
      this.timeWindowInMs = value;
      return this;
    }

    private void validate() {
      Objects.requireNonNull(pubSubTopicPartition, "pubSubTopicPartition");
      Objects.requireNonNull(topicManager, "topicManager");
      Objects.requireNonNull(splitType, "splitType");

      switch (splitType) {
        case CAPPED_SPLIT_COUNT:
          if (maxSplits == null || maxSplits <= 0) {
            maxSplits = DEFAULT_MAX_SPLITS;
          }
          break;

        case FIXED_RECORD_COUNT:
          if (recordsPerSplit == null || recordsPerSplit <= 0) {
            recordsPerSplit = DEFAULT_RECORDS_PER_SPLIT;
          }
          break;

        case TIME_WINDOW:
          if (timeWindowInMs == null || timeWindowInMs <= 0) {
            timeWindowInMs = DEFAULT_TIME_WINDOW_MS;
          }
          break;

        case WHOLE_PARTITION:
          // No extra parameters required.
          break;

        default:
          throw new IllegalStateException("Unknown SplitType: " + splitType);
      }
    }

    public SplitRequest build() {
      validate();
      return new SplitRequest(this);
    }
  }
}
