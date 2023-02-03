package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.adapter.ProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.VeniceProducerConfig;


public class ApacheKafkaProducerAdapterFactory implements ProducerAdapterFactory<ApacheKafkaProducerAdapter> {
  public static final String NAME = "ApacheKafka";

  @Override
  public ApacheKafkaProducerAdapter create(VeniceProducerConfig producerConfig) {
    return new ApacheKafkaProducerAdapter(producerConfig.getVeniceProperties());
  }
}
