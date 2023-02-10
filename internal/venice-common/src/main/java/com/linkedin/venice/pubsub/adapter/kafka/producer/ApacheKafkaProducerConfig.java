package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class holds all properties used to construct Kafka producers (this could be refactored to hold consumer properties as well).
 */
public class ApacheKafkaProducerConfig {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerConfig.class);

  public static final String KAFKA_CONFIG_PREFIX = "kafka.";
  public static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_CONFIG_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String KAFKA_PRODUCER_RETRIES_CONFIG = KAFKA_CONFIG_PREFIX + ProducerConfig.RETRIES_CONFIG;
  public static final String KAFKA_LINGER_MS = KAFKA_CONFIG_PREFIX + ProducerConfig.LINGER_MS_CONFIG;
  public static final String KAFKA_BATCH_SIZE = KAFKA_CONFIG_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG;
  public static final String KAFKA_BUFFER_MEMORY = KAFKA_CONFIG_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG;
  public static final String KAFKA_CLIENT_ID = KAFKA_CONFIG_PREFIX + ProducerConfig.CLIENT_ID_CONFIG;
  public static final String KAFKA_KEY_SERIALIZER = KAFKA_CONFIG_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
  public static final String KAFKA_VALUE_SERIALIZER =
      KAFKA_CONFIG_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
  public static final String KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS =
      KAFKA_CONFIG_PREFIX + ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
  public static final String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS =
      KAFKA_CONFIG_PREFIX + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;

  private final Properties producerProperties;

  public ApacheKafkaProducerConfig(Properties allVeniceProperties) {
    this(new VeniceProperties(allVeniceProperties), true);
  }

  public ApacheKafkaProducerConfig(VeniceProperties allVeniceProperties) {
    this(allVeniceProperties, true);
  }

  public ApacheKafkaProducerConfig(VeniceProperties allVeniceProperties, boolean strictConfigs) {
    this.producerProperties = validateAndExtractConfigs(allVeniceProperties, strictConfigs);
  }

  public Properties getProducerProperties() {
    return producerProperties;
  }

  /**
   * This class takes in all properties that begin with "{@value ApacheKafkaProducerConfig#KAFKA_CONFIG_PREFIX}" and emits the
   * rest of the properties.
   * It omits those properties that do not begin with "{@value ApacheKafkaProducerConfig#KAFKA_CONFIG_PREFIX}"
   */
  private Properties validateAndExtractConfigs(VeniceProperties allVeniceProperties, boolean strictConfigs) {
    if (!allVeniceProperties.containsKey(ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS)) {
      throw new ConfigurationException("Props key not found: " + ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS);
    }

    Properties kafkaProducerProperties =
        allVeniceProperties.clipAndFilterNamespace(ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX).toProperties();

    validateClassProp(
        kafkaProducerProperties,
        strictConfigs,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        KafkaKeySerializer.class.getName());
    validateClassProp(
        kafkaProducerProperties,
        strictConfigs,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        KafkaValueSerializer.class.getName());

    // This is to guarantee ordering, even in the face of failures.
    validateProp(kafkaProducerProperties, strictConfigs, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    // This will ensure the durability on Kafka broker side
    validateProp(kafkaProducerProperties, strictConfigs, ProducerConfig.ACKS_CONFIG, "all");

    if (!kafkaProducerProperties.containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
      kafkaProducerProperties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "300000"); // 5min
    }

    if (!kafkaProducerProperties.containsKey(ProducerConfig.RETRIES_CONFIG)) {
      kafkaProducerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    }

    if (!kafkaProducerProperties.contains(ProducerConfig.RETRY_BACKOFF_MS_CONFIG)) {
      // Hard-coded backoff config to be 1 sec
      validateProp(kafkaProducerProperties, strictConfigs, ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
    }

    if (!kafkaProducerProperties.containsKey(ProducerConfig.MAX_BLOCK_MS_CONFIG)) {
      // Block if buffer is full
      validateProp(
          kafkaProducerProperties,
          strictConfigs,
          ProducerConfig.MAX_BLOCK_MS_CONFIG,
          String.valueOf(Long.MAX_VALUE));
    }

    if (kafkaProducerProperties.containsKey(ProducerConfig.COMPRESSION_TYPE_CONFIG)) {
      LOGGER.info(
          "Compression type explicitly specified by config: {}",
          kafkaProducerProperties.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));
    } else {
      /**
       * In general, 'gzip' compression ratio is the best among all the available codecs:
       * 1. none
       * 2. lz4
       * 3. gzip
       * 4. snappy
       *
       * We want to minimize the cross-COLO bandwidth usage.
       */
      kafkaProducerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    }

    // Setup ssl config if needed.
    if (KafkaSSLUtils.validateAndCopyKafkaSSLConfig(allVeniceProperties, kafkaProducerProperties)) {
      LOGGER.info("Will initialize an SSL Kafka producer");
    } else {
      LOGGER.info("Will initialize a non-SSL Kafka producer");
    }

    return kafkaProducerProperties;
  }

  /**
   * Function which sets some required defaults. Also bubbles up an exception in
   * order to fail fast if any calling class tries to override these defaults.
   */
  private void validateProp(
      Properties properties,
      boolean strictConfigs,
      String requiredConfigKey,
      String requiredConfigValue) {
    String actualConfigValue = properties.getProperty(requiredConfigKey);
    if (actualConfigValue == null) {
      properties.setProperty(requiredConfigKey, requiredConfigValue);
    } else if (!actualConfigValue.equals(requiredConfigValue) && strictConfigs) {
      // We fail fast rather than attempting to use non-standard serializers
      throw new VeniceException(
          "The Kafka Producer must use certain configuration settings in order to work properly. "
              + "requiredConfigKey: '" + requiredConfigKey + "', requiredConfigValue: '" + requiredConfigValue
              + "', actualConfigValue: '" + actualConfigValue + "'.");
    }
  }

  /**
   * Validate and load Class properties.
   */
  private void validateClassProp(
      Properties properties,
      boolean strictConfigs,
      String requiredConfigKey,
      String requiredConfigValue) {
    validateProp(properties, strictConfigs, requiredConfigKey, requiredConfigValue);
    String className = properties.getProperty(requiredConfigKey);
    if (className == null) {
      return;
    }
    try {
      /*
       * The following code is trying to fix ClassNotFoundException while using JDK11.
       * Instead of letting Kafka lib load the specified class, application will load it on its own.
       * The difference is that Kafka lib is trying to load the specified class by `Thread.currentThread().getContextClassLoader()`,
       * which seems to be problematic with JDK11.
       */
      properties.put(requiredConfigKey, Class.forName(className));
    } catch (ClassNotFoundException e) {
      throw new VeniceClientException(
          "Failed to load the specified class: " + className + " for key: " + requiredConfigKey,
          e);
    }
  }
}
