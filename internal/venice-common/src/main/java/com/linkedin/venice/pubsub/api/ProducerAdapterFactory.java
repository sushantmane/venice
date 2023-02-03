package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.utils.VeniceProperties;


public interface ProducerAdapterFactory<ADAPTER extends ProducerAdapter> {
  ADAPTER create(VeniceProperties veniceProperties);
}
