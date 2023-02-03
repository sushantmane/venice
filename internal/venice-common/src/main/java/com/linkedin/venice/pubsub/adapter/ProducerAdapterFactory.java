package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.pubsub.api.ProducerConfig;


public interface ProducerAdapterFactory<VeniceProducer> {
  public VeniceProducer create(ProducerConfig producerConfig);
}
