package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.PubsubProducerCallback;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * A Kafka specific callback which wraps generic {@link PubsubProducerCallback}
 */
public class ApacheKafkaProducerCallback implements Callback {
  private final PubsubProducerCallback pubsubProducerCallback;

  public ApacheKafkaProducerCallback(PubsubProducerCallback pubsubProducerCallback) {
    this.pubsubProducerCallback = pubsubProducerCallback;
  }

  /**
   *
   * @param metadata The metadata for the record that was sent (i.e. the partition and offset). An empty metadata
   *                 with -1 value for all fields except for topicPartition will be returned if an error occurred.
   * @param exception The exception thrown during processing of this record. Null if no error occurred.
   *                  Possible thrown exceptions include:
   *
   *                  Non-Retriable exceptions (fatal, the message will never be sent):
   *
   *                  InvalidTopicException
   *                  OffsetMetadataTooLargeException
   *                  RecordBatchTooLargeException
   *                  RecordTooLargeException
   *                  UnknownServerException
   *
   *                  Retriable exceptions (transient, may be covered by increasing #.retries):
   *
   *                  CorruptRecordException
   *                  InvalidMetadataException
   *                  NotEnoughReplicasAfterAppendException
   *                  NotEnoughReplicasException
   *                  OffsetOutOfRangeException
   *                  TimeoutException
   *                  UnknownTopicOrPartitionException
   */
  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    pubsubProducerCallback.onCompletion(new ApacheKafkaProduceResult(metadata), exception);
  }
}
