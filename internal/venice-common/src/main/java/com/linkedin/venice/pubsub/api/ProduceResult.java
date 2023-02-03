package com.linkedin.venice.pubsub.api;

/**
 * An interface implemented by ProducerAdapters to return the result of produce action
 */
public interface ProduceResult {
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
