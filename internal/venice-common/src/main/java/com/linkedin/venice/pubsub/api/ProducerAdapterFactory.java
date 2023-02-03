package com.linkedin.venice.pubsub.api;

public interface ProducerAdapterFactory<ADAPTER extends ProducerAdapter> {
  ADAPTER create(ProducerConfig producerConfig);
}
