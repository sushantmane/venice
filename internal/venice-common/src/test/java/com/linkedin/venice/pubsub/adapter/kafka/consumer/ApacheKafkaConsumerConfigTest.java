package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_TO_KAFKA_LEGACY;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.testng.annotations.Test;


public class ApacheKafkaConsumerConfigTest {
  private static final String KAFKA_BROKER_ADDR = "kafka.broker.com:8181";
  private static final String SASL_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";

  private static final String SASL_MECHANISM = "PLAIN";

  @Test
  public void testSaslConfiguration() {
    Properties props = new Properties();
    props.put(SSL_TO_KAFKA_LEGACY, true);
    props.put(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BROKER_ADDR);
    props.put(SSL_KAFKA_BOOTSTRAP_SERVERS, "ssl.kafka.broker.com:8182");
    props.put("kafka.sasl.jaas.config", SASL_JAAS_CONFIG);
    props.put("kafka.sasl.mechanism", SASL_MECHANISM);
    props.put("kafka.security.protocol", "SASL_SSL");
    PubSubConsumerAdapterContext context =
        new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(props))
            .setConsumerName("test")
            .build();
    ApacheKafkaConsumerConfig consumerConfig = new ApacheKafkaConsumerConfig(context);
    Properties consumerProps = consumerConfig.getConsumerProperties();
    assertEquals(SASL_JAAS_CONFIG, consumerProps.get("sasl.jaas.config"));
    assertEquals(SASL_MECHANISM, consumerProps.get("sasl.mechanism"));
    assertEquals("SASL_SSL", consumerProps.get("security.protocol"));
  }

  @Test
  public void testGetValidConsumerProperties() {
    Properties allProps = new Properties();
    allProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
    allProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "2000");
    // this is common config; there are no admin specific configs
    allProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    allProps.put("bogus.kafka.config", "bogusValue");

    Properties validProps = ApacheKafkaConsumerConfig.getValidConsumerProperties(allProps);
    assertEquals(validProps.size(), 2);
    assertEquals(validProps.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG), "localhost:9092");
    assertEquals(validProps.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG), "2000");
  }

  @Test
  public void testDefaultsValuesAreUsedIfConfigIsNotProvided() {
    Properties props = new Properties();
    PubSubConsumerAdapterContext context =
        new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(props))
            .setConsumerName("test")
            .setBrokerAddress(KAFKA_BROKER_ADDR)
            .build();
    ApacheKafkaConsumerConfig apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(context);
    Properties consumerProps = apacheKafkaConsumerConfig.getConsumerProperties();
    assertNotNull(consumerProps);
    assertTrue(consumerProps.containsKey(ConsumerConfig.RECEIVE_BUFFER_CONFIG));
    assertEquals(
        consumerProps.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
        ApacheKafkaConsumerConfig.DEFAULT_RECEIVE_BUFFER_SIZE);

    props.put(ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.RECEIVE_BUFFER_CONFIG, "98765");
    context = new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(props))
        .setConsumerName("test")
        .setBrokerAddress(KAFKA_BROKER_ADDR)
        .build();
    apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(context);
    consumerProps = apacheKafkaConsumerConfig.getConsumerProperties();
    assertNotNull(consumerProps);
    assertTrue(consumerProps.containsKey(ConsumerConfig.RECEIVE_BUFFER_CONFIG));
    assertEquals(consumerProps.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG), "98765");
  }

  /**
   * Ensures that the Kafka consumer's key and value serializer configurations remain unchanged.
   * This test helps catch unintended modifications that could lead to serialization issues in
   * certain environments. A failure here indicates that the serializer configurations may work
   * in the test environment but could cause issues in other runtime environments.
   */
  @Test
  public void testKeyAndValueDeserializerConfigConsistency() {
    Properties props = new Properties();
    PubSubConsumerAdapterContext context =
        new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(props))
            .setConsumerName("test")
            .setBrokerAddress(KAFKA_BROKER_ADDR)
            .build();
    ApacheKafkaConsumerConfig apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(context);
    Properties consumerProps = apacheKafkaConsumerConfig.getConsumerProperties();
    assertNotNull(consumerProps);

    // Ensure the deserializer is not incorrectly set as a String
    assertNotEquals(consumerProps.get(KEY_DESERIALIZER_CLASS_CONFIG), ByteArrayDeserializer.class.getName());
    assertFalse(
        consumerProps.get(KEY_DESERIALIZER_CLASS_CONFIG) instanceof String,
        "Key deserializer should not be a string class name");

    assertNotEquals(consumerProps.get(VALUE_DESERIALIZER_CLASS_CONFIG), ByteArrayDeserializer.class.getName());
    assertFalse(
        consumerProps.get(VALUE_DESERIALIZER_CLASS_CONFIG) instanceof String,
        "Value deserializer should not be a string class name");

    // Ensure the deserializer is set as a class
    assertEquals(consumerProps.get(KEY_DESERIALIZER_CLASS_CONFIG), ByteArrayDeserializer.class);
    assertEquals(consumerProps.get(VALUE_DESERIALIZER_CLASS_CONFIG), ByteArrayDeserializer.class);
  }
}
