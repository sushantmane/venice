package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;


public interface ProducerAdapterFactory<ADAPTER extends ProducerAdapter> extends Closeable {
  /**
   *
   * @param veniceProperties      A copy of venice properties. Relevant producer configs will be extracted
   *                              from veniceProperties using prefix matching. For kafka related producer configs
   *                              kafka, ApacheKafkaProducerConfig will extract all the configs that start with "kafka."
   *                              prefix.
   * @param producerName          Name of the producer. If not null used as client.id in case of kafka pubsub producer.
   * @param targetBrokerAddress   Broker address to use when creating a producer. If this value is null, localBrokerAddress
   *                              from the producer factory will be used.
   * @return
   */
  ADAPTER create(VeniceProperties veniceProperties, String producerName, String targetBrokerAddress);
}
