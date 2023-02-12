package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_TO_KAFKA;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.ProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;


public class ApacheKafkaProducerAdapterFactory implements ProducerAdapterFactory<ApacheKafkaProducerAdapter> {
  public static final String NAME = "ApacheKafka";

  @Override
  public ApacheKafkaProducerAdapter create(String topicName, VeniceProperties veniceProperties) {
    return new ApacheKafkaProducerAdapter(new ApacheKafkaProducerConfig(veniceProperties, true));
  }

  @Override
  public String getPubsubBrokerAddress(Properties properties) {
    if (Boolean.parseBoolean(properties.getProperty(SSL_TO_KAFKA, "false"))) {
      checkProperty(properties, SSL_KAFKA_BOOTSTRAP_SERVERS);
      return properties.getProperty(SSL_KAFKA_BOOTSTRAP_SERVERS);
    }
    checkProperty(properties, KAFKA_BOOTSTRAP_SERVERS);
    return properties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
  }

  private void checkProperty(Properties properties, String key) {
    if (!properties.containsKey(key)) {
      throw new VeniceException(
          "Invalid properties for Kafka producer factory. Required property: " + key + " is missing.");
    }
  }

  @Override
  public void close() {
  }
}
