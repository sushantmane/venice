package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.LinkedBlockingQueue;


public class TopicManagerNew {
  public TopicManagerNew(TopicManagerContext topicManagerContext, String pubSubClusterAddress) {
    this.topicMetadataFetcherPool = new LinkedBlockingQueue<>(topicManagerContext.getTopicMetadataFetcherPoolSize());
    for (int i = 0; i < topicManagerContext.getTopicMetadataFetcherPoolSize(); i++) {
      TopicMetadataFetcher topicMetadataFetcher = new TopicMetadataFetcher(
          pubSubAdminAdapterShared,
          null, // TODO: add pubSubConsumerAdapter
          pubSubClusterAddress);
      if (!topicMetadataFetcherPool.offer(topicMetadataFetcher)) {
        throw new VeniceException("Failed to initialize topicMetadataFetcherPool");
      }
    }
  }
}
