package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * A Kafka specific callback which wraps generic {@link PubSubProducerCallback}
 */
public class ApacheKafkaProducerCallback implements Callback {
  private final PubSubProducerCallback pubsubProducerCallback;
  private final CompletableFuture<PubSubProduceResult> produceResultFuture = new CompletableFuture<>();
  private final KafkaProducer kafkaProducer;
  private final Boolean closeNow;

  public ApacheKafkaProducerCallback(PubSubProducerCallback pubsubProducerCallback) {
    this(pubsubProducerCallback, null, false);
  }

  public ApacheKafkaProducerCallback(
      PubSubProducerCallback pubsubProducerCallback,
      KafkaProducer kafkaProducer,
      Boolean closeNow) {
    this.pubsubProducerCallback = pubsubProducerCallback;
    this.kafkaProducer = kafkaProducer;
    this.closeNow = closeNow;
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
    if (kafkaProducer != null && closeNow) {
      kafkaProducer.close(Duration.ZERO);
      return;
    }

    PubSubProduceResult produceResult = new ApacheKafkaProduceResult(metadata);
    if (exception != null) {
      produceResultFuture.completeExceptionally(exception);
    } else {
      produceResultFuture.complete(produceResult);
    }
    pubsubProducerCallback.onCompletion(new ApacheKafkaProduceResult(metadata), exception);
  }

  Future<PubSubProduceResult> getProduceResultFuture() {
    return produceResultFuture;
  }
}
