package com.linkedin.venice.pubsub.api;

/**
 * An interface implemented by specific PubsubProducerAdapters to return the result of a produce action.
 */
public interface PubsubProduceResult {
  /**
   * The offset of the record in the topic/partition.
   */
  long offset();

  /**
   * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
   * is -1.
   */
  int serializedKeySize();

  /**
   * The size of the serialized, uncompressed value in bytes. If value is null, the returned
   * size is -1.
   */
  int serializedValueSize();

  /**
   * The topic the record was appended to
   */
  String topic();

  /**
   * The partition the record was sent to
   */
  int partition();
}
