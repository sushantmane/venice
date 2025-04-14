package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;


public class PubSubPositionResolverContext {
  private final TopicManagerRepository topicManagerRepository;
  private final Int2ObjectMap<String> clusterIdToBrokerAddressMap;
  private final Int2ObjectMap<String> typeIdToClassNameMap;

  private PubSubPositionResolverContext(Builder builder) {
    this.topicManagerRepository = builder.topicManagerRepository;
    this.clusterIdToBrokerAddressMap = builder.clusterIdToBrokerAddressMap;
    this.typeIdToClassNameMap = builder.typeIdToClassNameMap;
  }

  public TopicManagerRepository getTopicManagerRepository() {
    return topicManagerRepository;
  }

  public Int2ObjectMap<String> getPubSubClusterIdToUrlMap() {
    return clusterIdToBrokerAddressMap;
  }

  public Int2ObjectMap<String> getTypeIdToClassNameMap() {
    return typeIdToClassNameMap;
  }

  public static class Builder {
    private TopicManagerRepository topicManagerRepository;
    private Int2ObjectMap<String> clusterIdToBrokerAddressMap;
    private Int2ObjectMap<String> typeIdToClassNameMap;

    public Builder setTopicManagerRepository(TopicManagerRepository topicManagerRepository) {
      this.topicManagerRepository = topicManagerRepository;
      return this;
    }

    public Builder setClusterIdToBrokerAddressMap(Int2ObjectMap<String> clusterIdToBrokerAddressMap) {
      this.clusterIdToBrokerAddressMap = clusterIdToBrokerAddressMap;
      return this;
    }

    public Builder setTypeIdToClassNameMap(Int2ObjectMap<String> typeIdToClassNameMap) {
      this.typeIdToClassNameMap = typeIdToClassNameMap;
      return this;
    }

    public PubSubPositionResolverContext build() {
      if (topicManagerRepository == null) {
        throw new IllegalArgumentException("TopicManagerRepository cannot be null");
      }

      if (clusterIdToBrokerAddressMap == null) {
        throw new IllegalArgumentException("Cluster ID to Broker Address Map cannot be null");
      }

      if (typeIdToClassNameMap == null) {
        throw new IllegalArgumentException("Type ID to Class Name Map cannot be null");
      }
      return new PubSubPositionResolverContext(this);
    }
  }
}
