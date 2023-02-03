package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.PubsubProducerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;


public class ApacheKafkaProducerConfig implements PubsubProducerConfig {
  public static final String KAFKA_CONFIG_PREFIX = "kafka.";
  public static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_CONFIG_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String KAFKA_PRODUCER_RETRIES_CONFIG = KAFKA_CONFIG_PREFIX + ProducerConfig.RETRIES_CONFIG;
  public static final String KAFKA_LINGER_MS = KAFKA_CONFIG_PREFIX + ProducerConfig.LINGER_MS_CONFIG;
  public static final String KAFKA_BATCH_SIZE = KAFKA_CONFIG_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG;
  public static final String KAFKA_KEY_SERIALIZER = KAFKA_CONFIG_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
  public static final String KAFKA_VALUE_SERIALIZER =
      KAFKA_CONFIG_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
  public static final String KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS =
      KAFKA_CONFIG_PREFIX + ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
  public static final String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS =
      KAFKA_CONFIG_PREFIX + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
}
