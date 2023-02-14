package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.pubsub.api.ProduceResult;


/**
 * A simple implementation of ProduceResult interface for testing purposes.
 */
public class SimpleProduceResultImpl implements ProduceResult {
  private final String topic;
  private final int partition;
  private final long offset;
  private final int serializedKeySize;
  private final int serializedValueSize;

  public SimpleProduceResultImpl(
      String topic,
      int partition,
      long offset,
      int serializedKeySize,
      int serializedValueSize) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.serializedKeySize = serializedKeySize;
    this.serializedValueSize = serializedValueSize;
  }

  @Override
  public long offset() {
    return offset;
  }

  @Override
  public int serializedKeySize() {
    return serializedKeySize;
  }

  @Override
  public int serializedValueSize() {
    return serializedValueSize;
  }

  @Override
  public String topic() {
    return topic;
  }

  @Override
  public int partition() {
    return partition;
  }
}
