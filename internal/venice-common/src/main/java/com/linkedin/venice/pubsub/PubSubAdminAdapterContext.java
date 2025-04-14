package com.linkedin.venice.pubsub;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;

import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;


public class PubSubAdminAdapterContext {
  private final String pubSubBrokerAddress;
  private final String adminClientName;
  private final VeniceProperties properties;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final MetricsRepository metricsRepository;

  private PubSubAdminAdapterContext(Builder builder) {
    this.pubSubBrokerAddress = builder.pubSubBrokerAddress;
    this.adminClientName = builder.adminClientName;
    this.properties = builder.properties;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.metricsRepository = builder.metricsRepository;
  }

  public String getPubSubBrokerAddress() {
    return pubSubBrokerAddress;
  }

  public VeniceProperties getProperties() {
    return properties;
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  public String getAdminClientName() {
    return adminClientName;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public static class Builder {
    private String pubSubBrokerAddress;
    private String adminClientName;
    private VeniceProperties properties;
    private PubSubTopicRepository pubSubTopicRepository;
    private MetricsRepository metricsRepository;

    public Builder setPubSubBrokerAddress(String pubSubBrokerAddress) {
      this.pubSubBrokerAddress = pubSubBrokerAddress;
      return this;
    }

    public Builder setAdminClientName(String adminClientName) {
      this.adminClientName = adminClientName;
      return this;
    }

    public Builder setProperties(VeniceProperties properties) {
      this.properties = properties;
      return this;
    }

    public Builder setPubSubTopicRepository(PubSubTopicRepository pubSubTopicRepository) {
      this.pubSubTopicRepository = pubSubTopicRepository;
      return this;
    }

    public Builder setMetricsRepository(MetricsRepository metricsRepository) {
      this.metricsRepository = metricsRepository;
      return this;
    }

    public PubSubAdminAdapterContext build() {
      if (pubSubBrokerAddress == null) {
        // TODO (sushantmane): throw an exception if pubSubBrokerAddress is null
        pubSubBrokerAddress = PubSubUtil.getPubSubBrokerAddress(properties, null);
        if (pubSubBrokerAddress == null) {
          throw new IllegalArgumentException(
              "Missing required broker address. Please specify either '" + PUBSUB_BROKER_ADDRESS + "' or '"
                  + KAFKA_BOOTSTRAP_SERVERS + "' in the configuration.");
        }
      }

      if (properties == null) {
        throw new IllegalArgumentException("Missing required properties. Please specify the properties.");
      }

      if (pubSubTopicRepository == null) {
        throw new IllegalArgumentException("Missing required topic repository. Please specify the topic repository.");
      }

      if (adminClientName == null) {
        adminClientName = PubSubUtil.generatePubSubClientId(PubSubClientType.ADMIN, null, pubSubBrokerAddress);
      }

      return new PubSubAdminAdapterContext(this);
    }
  }
}
