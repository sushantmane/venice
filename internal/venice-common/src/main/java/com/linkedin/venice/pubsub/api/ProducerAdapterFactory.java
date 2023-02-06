package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;


public interface ProducerAdapterFactory<ADAPTER extends ProducerAdapter> extends Closeable {
  ADAPTER create(String topic, VeniceProperties veniceProperties);

  default ADAPTER create(VeniceProperties veniceProperties) {
    return create(null, veniceProperties);
  }
}
