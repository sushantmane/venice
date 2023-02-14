package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.ProduceResult;
import java.util.Objects;
import org.apache.kafka.clients.producer.RecordMetadata;


public class ApacheKafkaProduceResult implements ProduceResult {
  private final RecordMetadata recordMetadata;

  public ApacheKafkaProduceResult(RecordMetadata recordMetadata) {
    this.recordMetadata = Objects.requireNonNull(recordMetadata, "RecordMetadata cannot be null");
  }

  /**
   * The topic the record was appended to
   */
  public String topic() {
    return recordMetadata.topic();
  }

  /**
   * The partition the record was sent to
   */
  public int partition() {
    return recordMetadata.partition();
  }

  /**
   * The offset of the record in the topic/partition.
   */
  public long offset() {
    return recordMetadata.offset();
  }

  /**
   * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
   * is -1.
   */
  public int serializedKeySize() {
    return recordMetadata.serializedKeySize();
  }

  /**
   * The size of the serialized, uncompressed value in bytes. If value is null, the returned
   * size is -1.
   */
  public int serializedValueSize() {
    return recordMetadata.serializedValueSize();
  }

  @Override
  public String toString() {
    return recordMetadata.topic() + ":" + recordMetadata.partition() + "@" + recordMetadata.offset();
  }

}
