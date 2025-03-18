package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.internals.RecordHeaders;


public class ApacheKafkaUtils {
  protected static final RecordHeaders EMPTY_RECORD_HEADERS = new RecordHeaders();

  static {
    EMPTY_RECORD_HEADERS.setReadOnly();
  }

  public static RecordHeaders convertToKafkaSpecificHeaders(PubSubMessageHeaders headers) {
    if (headers == null || headers.isEmpty()) {
      return EMPTY_RECORD_HEADERS;
    }
    RecordHeaders recordHeaders = new RecordHeaders();
    for (PubSubMessageHeader header: headers) {
      recordHeaders.add(header.key(), header.value());
    }
    return recordHeaders;
  }

  /**
   * This function will extract SSL related config if Kafka SSL is enabled.
   *
   * @param veniceProperties
   * @param properties
   * @return whether Kafka SSL is enabled or not0
   */
  public static boolean validateAndCopyKafkaSSLConfig(VeniceProperties veniceProperties, Properties properties) {
    if (!veniceProperties.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
      // No security protocol specified
      return false;
    }
    String kafkaProtocol = veniceProperties.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    if (!isKafkaProtocolValid(kafkaProtocol)) {
      throw new VeniceException("Invalid Kafka protocol specified: " + kafkaProtocol);
    }
    if (!isKafkaSSLProtocol(kafkaProtocol)) {
      // TLS/SSL is not enabled
      return false;
    }
    // Since SSL is enabled, the following configs are mandatory
    KAFKA_SSL_MANDATORY_CONFIGS.forEach(config -> {
      if (!veniceProperties.containsKey(config)) {
        throw new VeniceException(config + " is required when Kafka SSL is enabled");
      }
      properties.setProperty(config, veniceProperties.getString(config));
    });
    return true;
  }

  /**
   * Mandatory Kafka SSL configs when SSL is enabled.
   */
  private static final List<String> KAFKA_SSL_MANDATORY_CONFIGS = Arrays.asList(
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
      SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
      SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
      SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
      SslConfigs.SSL_KEY_PASSWORD_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
      SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
      SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
      SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);

  public static boolean isKafkaSSLProtocol(PubSubSecurityProtocol kafkaProtocol) {
    return kafkaProtocol == PubSubSecurityProtocol.SSL || kafkaProtocol == PubSubSecurityProtocol.SASL_SSL;
  }

  public static boolean isKafkaProtocolValid(String kafkaProtocol) {
    return kafkaProtocol.equals(PubSubSecurityProtocol.PLAINTEXT.name())
        || kafkaProtocol.equals(PubSubSecurityProtocol.SSL.name())
        || kafkaProtocol.equals(PubSubSecurityProtocol.SASL_PLAINTEXT.name())
        || kafkaProtocol.equals(PubSubSecurityProtocol.SASL_SSL.name());
  }

  public static boolean isKafkaSSLProtocol(String kafkaProtocol) {
    return kafkaProtocol.equals(PubSubSecurityProtocol.SSL.name())
        || kafkaProtocol.equals(PubSubSecurityProtocol.SASL_SSL.name());
  }

  public static PubSubClientsFactory getKafkaClientsFactory() {
    return new PubSubClientsFactory(
        new ApacheKafkaProducerAdapterFactory(),
        new ApacheKafkaConsumerAdapterFactory(),
        new ApacheKafkaAdminAdapterFactory());
  }
}
