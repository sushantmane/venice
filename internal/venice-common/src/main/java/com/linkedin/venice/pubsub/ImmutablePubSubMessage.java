package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.PubsubMessageHeaders;


public class ImmutablePubSubMessage<K, V> implements PubSubMessage<K, V, Long> {
  private final K key;
  private final V value;
  private final PubSubTopicPartition topicPartition;
  private final long offset;
  private final long timestamp;
  private final int payloadSize;
  private final PubsubMessageHeaders headers;

  public ImmutablePubSubMessage(
      K key,
      V value,
      PubSubTopicPartition topicPartition,
      long offset,
      long timestamp,
      int payloadSize) {
    this(key, value, topicPartition, offset, timestamp, payloadSize, null);
  }

  public ImmutablePubSubMessage(
      K key,
      V value,
      PubSubTopicPartition topicPartition,
      long offset,
      long timestamp,
      int payloadSize,
      PubsubMessageHeaders headers) {
    this.key = key;
    this.value = value;
    this.topicPartition = topicPartition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.payloadSize = payloadSize;
    this.headers = headers;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public PubSubTopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public Long getOffset() {
    return offset;
  }

  @Override
  public long getPubSubMessageTime() {
    return timestamp;
  }

  @Override
  public int getPayloadSize() {
    return payloadSize;
  }

  @Override
  public PubsubMessageHeaders getHeaders() {
    return headers;
  }
}
