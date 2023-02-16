package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.PubsubProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;


/**
 * Implementation of {@link PubsubProducerAdapterFactory} used to create Apache Kafka producers.
 *
 * A producer created using this factory is usually used to send data to a single pub-sub topic.
 */
public class ApacheKafkaProducerAdapterFactory implements PubsubProducerAdapterFactory<ApacheKafkaProducerAdapter> {
  private static final String NAME = "ApacheKafkaProducer";

  @Override
  public ApacheKafkaProducerAdapter create(
      VeniceProperties veniceProperties,
      String producerName,
      String brokerAddressToOverride) {
    return new ApacheKafkaProducerAdapter(
        new ApacheKafkaProducerConfig(veniceProperties, brokerAddressToOverride, producerName, true));
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() {
  }
}
