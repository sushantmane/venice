package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.PubsubProducerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;


public class SharedKafkaProducerConfig implements PubsubProducerConfig {

  // This helps override kafka config for shared producer separately than dedicated producer.
  public static final String SHARED_KAFKA_PRODUCER_CONFIG_PREFIX = "shared.producer.";
  public static final String SHARED_KAFKA_PRODUCER_BATCH_SIZE =
      SHARED_KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG;
}
