package com.linkedin.venice.writer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.pubsub.api.PubsubProduceResult;
import com.linkedin.venice.pubsub.api.PubsubProducerCallback;
import org.apache.logging.log4j.Logger;


class KafkaMessageCallback implements PubsubProducerCallback {
  private final KafkaMessageEnvelope value;
  private final Logger logger;

  public KafkaMessageCallback(KafkaMessageEnvelope value, Logger logger) {
    this.value = value;
    this.logger = logger;
  }

  @Override
  public void onCompletion(PubsubProduceResult produceResult, Exception e) {
    if (e != null) {
      logger.error(
          "Failed to send out message to Kafka producer: [value.messageType: {}, value.producerMetadata: {}]",
          value.messageType,
          value.producerMetadata,
          e);
    }
  }
}
