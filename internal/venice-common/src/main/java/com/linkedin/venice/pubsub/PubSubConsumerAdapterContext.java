package com.linkedin.venice.pubsub;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;

import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;


/**
 * Encapsulates all dependencies and configurations required to create a consumer using a consumer factory.
 * This class serves as a unified context for managing consumer setup across different PubSub systems.
 *
 * <p>Common configurations applicable to all PubSub implementations should be defined as member variables
 * in this class, while system-specific configurations should be stored in {@link VeniceProperties}.</p>
 *
 * <p>Each PubSub implementation is expected to interpret both the common configurations and the
 * PubSub-specific settings based on namespace-scoped configurations.</p>
 */
public class PubSubConsumerAdapterContext {
  private final String consumerName;
  private final String brokerAddress;
  private final VeniceProperties veniceProperties;
  private final PubSubSecurityProtocol securityProtocol;
  private final MetricsRepository metricsRepository;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final PubSubMessageDeserializer pubSubMessageDeserializer;
  private final boolean isOffsetCollectionEnabled;

  private PubSubConsumerAdapterContext(Builder builder) {
    this.consumerName = builder.consumerName;
    this.brokerAddress = builder.brokerAddress;
    this.veniceProperties = builder.veniceProperties;
    this.securityProtocol = builder.securityProtocol;
    this.metricsRepository = builder.metricsRepository;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.isOffsetCollectionEnabled = builder.isOffsetCollectionEnabled;
    this.pubSubMessageDeserializer = builder.pubSubMessageDeserializer;
  }

  public String getConsumerName() {
    return consumerName;
  }

  public String getBrokerAddress() {
    return brokerAddress;
  }

  public VeniceProperties getVeniceProperties() {
    return veniceProperties;
  }

  public PubSubSecurityProtocol getSecurityProtocol() {
    return securityProtocol;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  public boolean isOffsetCollectionEnabled() {
    return isOffsetCollectionEnabled;
  }

  public PubSubMessageDeserializer getPubSubMessageDeserializer() {
    return pubSubMessageDeserializer;
  }

  public static class Builder {
    private String consumerName;
    private String brokerAddress;
    private VeniceProperties veniceProperties;
    private PubSubSecurityProtocol securityProtocol;
    private MetricsRepository metricsRepository;
    private PubSubTopicRepository pubSubTopicRepository;
    private boolean isOffsetCollectionEnabled;
    private PubSubMessageDeserializer pubSubMessageDeserializer;

    public Builder setConsumerName(String consumerName) {
      this.consumerName = consumerName;
      return this;
    }

    public Builder setBrokerAddress(String brokerAddress) {
      this.brokerAddress = brokerAddress;
      return this;
    }

    public Builder setVeniceProperties(VeniceProperties veniceProperties) {
      this.veniceProperties = veniceProperties;
      return this;
    }

    public Builder setSecurityProtocol(PubSubSecurityProtocol securityProtocol) {
      this.securityProtocol = securityProtocol;
      return this;
    }

    public Builder setMetricsRepository(MetricsRepository metricsRepository) {
      this.metricsRepository = metricsRepository;
      return this;
    }

    public Builder setPubSubTopicRepository(PubSubTopicRepository pubSubTopicRepository) {
      this.pubSubTopicRepository = pubSubTopicRepository;
      return this;
    }

    public Builder setIsOffsetCollectionEnabled(boolean shouldValidateProducerConfigStrictly) {
      this.isOffsetCollectionEnabled = shouldValidateProducerConfigStrictly;
      return this;
    }

    public Builder setPubSubMessageDeserializer(PubSubMessageDeserializer pubSubMessageDeserializer) {
      this.pubSubMessageDeserializer = pubSubMessageDeserializer;
      return this;
    }

    public PubSubConsumerAdapterContext build() {
      if (brokerAddress == null) {
        // TODO (sushantmane): Once all callers are updated to use the builder, do not lookup the broker address from
        // properties. Instead, throw an exception if the broker address is not provided.
        brokerAddress = PubSubUtil.getPubSubBrokerAddress(veniceProperties, null);
        if (brokerAddress == null) {
          throw new IllegalArgumentException(
              "Missing required broker address. Please specify either '" + PUBSUB_BROKER_ADDRESS + "' or '"
                  + KAFKA_BOOTSTRAP_SERVERS + "' in the configuration.");
        }
      }
      if (consumerName == null) {
        consumerName = PubSubUtil.generatePubSubClientId(PubSubClientType.CONSUMER, null, brokerAddress);
      }
      return new PubSubConsumerAdapterContext(this);
    }
  }
}
