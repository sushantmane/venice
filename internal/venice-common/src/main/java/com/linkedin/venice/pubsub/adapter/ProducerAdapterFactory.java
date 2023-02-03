package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.pubsub.api.VeniceProducer;
import com.linkedin.venice.pubsub.api.VeniceProducerConfig;


public interface ProducerAdapterFactory<VeniceProducer> {
  public VeniceProducer create(VeniceProducerConfig producerConfig);
}
