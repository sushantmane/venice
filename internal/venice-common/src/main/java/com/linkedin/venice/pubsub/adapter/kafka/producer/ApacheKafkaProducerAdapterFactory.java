package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.ProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.ProducerConfig;


public class ApacheKafkaProducerAdapterFactory implements ProducerAdapterFactory<ApacheKafkaProducerAdapter> {
  public static final String NAME = "ApacheKafka";

  @Override
  public ApacheKafkaProducerAdapter create(ProducerConfig producerConfig) {
    return new ApacheKafkaProducerAdapter(producerConfig.getVeniceProperties());
  }
}
