package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.adapter.SimplePubsubProduceResultImpl;
import com.linkedin.venice.pubsub.api.PubsubProduceResult;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * Converts RecordMetadata to {@link PubsubProduceResult}
 */
public class ApacheKafkaProduceResult extends SimplePubsubProduceResultImpl {
  public ApacheKafkaProduceResult(RecordMetadata recordMetadata) {
    super(
        recordMetadata.topic(),
        recordMetadata.partition(),
        recordMetadata.offset(),
        recordMetadata.serializedKeySize(),
        recordMetadata.serializedValueSize());
  }
}
