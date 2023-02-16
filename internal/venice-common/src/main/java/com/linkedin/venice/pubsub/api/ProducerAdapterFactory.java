package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;


/**
 * A producer factory interface that should be implemented to support writing venice data to a
 */
public interface ProducerAdapterFactory<ADAPTER extends ProducerAdapter> extends Closeable {
  /**
   *
   * @param veniceProperties     A copy of venice properties. Relevant producer configs will be extracted from
   *                             veniceProperties using prefix matching. For example, to construct kafka producer
   *                             configs that start with "kafka." prefix will be used.
   * @param producerName         Name of the producer. If not null, it will be used to set the context
   *                             for producer thread.
   * @param targetBrokerAddress  Broker address to use when creating a producer.
   *                             If this value is null, local broker address present in veniceProperties will be used.
   * @return                     Returns an instance of a producer adapter
   */
  ADAPTER create(VeniceProperties veniceProperties, String producerName, String targetBrokerAddress);
}
