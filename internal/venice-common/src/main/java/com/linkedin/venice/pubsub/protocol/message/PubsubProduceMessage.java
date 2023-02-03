package com.linkedin.venice.pubsub.protocol.message;

import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.PubsubMessageHeaders;


public class PubsubProduceMessage<K, V> implements PubSubMessage<K, V, Long> {
  private final K key;
  private final V value;
  private final PubSubTopicPartition topicPartition;
  private final long offset;
  private final long timestamp;
  private final int payloadSize;
  private final PubsubMessageHeaders headers;

  public PubsubProduceMessage(Builder<K, V> builder) {
    this.key = builder.key;
    this.value = builder.value;
    this.topicPartition = builder.topicPartition;
    this.offset = builder.offset;
    this.timestamp = builder.timestamp;
    this.payloadSize = builder.payloadSize;
    this.headers = builder.headers;
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

  public static class Builder<K, V> {
    private K key;
    private V value;
    private PubSubTopicPartition topicPartition;
    private String topic;
    private int partition;
    private long offset;
    private long timestamp;
    private int payloadSize;
    private PubsubMessageHeaders headers;

    public Builder<K, V> key(K key) {
      this.key = key;
      return this;
    }

    public Builder<K, V> value(V value) {
      this.value = value;
      return this;
    }

    public Builder<K, V> topicPartition(PubSubTopicPartition topicPartition) {
      this.topicPartition = topicPartition;
      return this;
    }

    public Builder<K, V> offset(long offset) {
      this.offset = offset;
      return this;
    }

    public Builder<K, V> timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder<K, V> payloadSize(int payloadSize) {
      this.payloadSize = payloadSize;
      return this;
    }

    public Builder<K, V> headers(PubsubMessageHeaders headers) {
      this.headers = headers;
      return this;
    }

    public PubsubProduceMessage<K, V> build() {
      return new PubsubProduceMessage<>(this);
    }
  }
}
