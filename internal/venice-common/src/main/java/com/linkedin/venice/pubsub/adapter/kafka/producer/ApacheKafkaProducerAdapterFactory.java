package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.ProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;


public class ApacheKafkaProducerAdapterFactory implements ProducerAdapterFactory<ApacheKafkaProducerAdapter> {
  public static final String NAME = "ApacheKafka";

  @Override
  public ApacheKafkaProducerAdapter create(VeniceProperties veniceProperties) {
    return new ApacheKafkaProducerAdapter(veniceProperties);
  }
}
