package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;


class TopicMetadataFetcherContext {
  private final String pubSubClusterAddress;
  private final Lazy<PubSubAdminAdapter> pubSubAdminAdapterLazy;
  private final PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory;
  private final PubSubMessageDeserializer pubSubMessageDeserializer;
  private final VeniceProperties pubSubProperties;
  private final int fetcherId;

  private TopicMetadataFetcherContext(Builder builder) {
    this.pubSubClusterAddress = builder.pubSubClusterAddress;
    this.pubSubAdminAdapterLazy = builder.pubSubAdminAdapterLazy;
    this.pubSubConsumerAdapterFactory = builder.pubSubConsumerAdapterFactory;
    this.pubSubProperties = builder.pubSubProperties;
    this.pubSubMessageDeserializer = builder.pubSubMessageDeserializer;
    this.fetcherId = builder.fetcherId;
  }

  public String getPubSubClusterAddress() {
    return pubSubClusterAddress;
  }

  public Lazy<PubSubAdminAdapter> getPubSubAdminAdapterLazy() {
    return pubSubAdminAdapterLazy;
  }

  public PubSubConsumerAdapterFactory getPubSubConsumerAdapterFactory() {
    return pubSubConsumerAdapterFactory;
  }

  public VeniceProperties getPubSubProperties(String pubSubClusterAddress) {
    return pubSubProperties;
  }

  public PubSubMessageDeserializer getPubSubMessageDeserializer() {
    return pubSubMessageDeserializer;
  }

  public int getFetcherId() {
    return fetcherId;
  }

  static class Builder {
    private String pubSubClusterAddress;
    private VeniceProperties pubSubProperties;
    private Lazy<PubSubAdminAdapter> pubSubAdminAdapterLazy;
    private PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory;
    private PubSubMessageDeserializer pubSubMessageDeserializer;
    private int fetcherId = -1;

    public Builder setFetcherId(int fetcherId) {
      this.fetcherId = fetcherId;
      return this;
    }

    public Builder setPubSubClusterAddress(String pubSubClusterAddress) {
      this.pubSubClusterAddress = pubSubClusterAddress;
      return this;
    }

    public Builder setPubSubAdminAdapterLazy(Lazy<PubSubAdminAdapter> pubSubAdminAdapterLazy) {
      this.pubSubAdminAdapterLazy = pubSubAdminAdapterLazy;
      return this;
    }

    public Builder setPubSubConsumerAdapterFactory(PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory) {
      this.pubSubConsumerAdapterFactory = pubSubConsumerAdapterFactory;
      return this;
    }

    public Builder setPubSubProperties(VeniceProperties pubSubProperties) {
      this.pubSubProperties = pubSubProperties;
      return this;
    }

    public Builder setPubSubMessageDeserializer(PubSubMessageDeserializer pubSubMessageDeserializer) {
      this.pubSubMessageDeserializer = pubSubMessageDeserializer;
      return this;
    }

    TopicMetadataFetcherContext build() {
      return new TopicMetadataFetcherContext(this);
    }
  }
}
