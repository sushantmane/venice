package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


public interface PubSubConsumerAdapter extends AutoCloseable, Closeable {
  void subscribe(PubSubTopicPartition pubSubTopicPartition, long lastReadOffset);

  void unSubscribe(PubSubTopicPartition pubSubTopicPartition);

  void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionSet);

  void resetOffset(PubSubTopicPartition pubSubTopicPartition) throws PubSubUnsubscribedTopicPartitionException;

  void close();

  Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> poll(long timeoutMs);

  /**
   * @return True if this consumer has subscribed any pub sub topic partition at all and vice versa.
   */
  boolean hasAnySubscription();

  boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition);

  void pause(PubSubTopicPartition pubSubTopicPartition);

  void resume(PubSubTopicPartition pubSubTopicPartition);

  Set<PubSubTopicPartition> getAssignment();

  /**
   * Get consumer offset lag for a pub sub topic partition based on metrics. This may not be
   * 100% accurate, but it is a good approximation.
   * @return an offset lag of zero or above if a valid lag was collected by the consumer, or -1 otherwise
   */
  default long getConsumerLagBasedOnMetrics(PubSubTopicPartition pubSubTopicPartition) {
    return -1;
  }

  @Deprecated
  /**
   * @deprecated use {@link #getConsumerLagBasedOnMetrics(PubSubTopicPartition)} instead
   */
  default long getOffsetLag(PubSubTopicPartition pubSubTopicPartition) {
    return getConsumerLagBasedOnMetrics(pubSubTopicPartition);
  }

  /**
   * Get the latest/end offset for a topic partition based on metrics. This may not be
   * 100% accurate, but it is a good approximation.
   * @return the latest offset (zero or above) if an offset was collected by the consumer, or -1 otherwise
   */
  default long getEndOffsetBasedOnMetrics(PubSubTopicPartition pubSubTopicPartition) {
    return -1;
  }

  @Deprecated
  /**
   * @deprecated use {@link #getEndOffsetBasedOnMetrics(PubSubTopicPartition)} instead
   */
  default long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    return getEndOffsetBasedOnMetrics(pubSubTopicPartition);
  }

  Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp, Duration timeout);

  Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp);

  Long beginningOffset(PubSubTopicPartition partition, Duration timeout);

  Map<PubSubTopicPartition, Long> endOffsets(Collection<PubSubTopicPartition> partitions, Duration timeout);

  Long endOffset(PubSubTopicPartition pubSubTopicPartition);

  List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic);
}
