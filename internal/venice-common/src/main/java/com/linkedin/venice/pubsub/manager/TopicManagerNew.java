package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class TopicManagerNew {
  private final TopicManagerContext topicManagerContext;
  private final BlockingQueue<TopicMetadataFetcher> topicMetadataFetcherPool;
  private final String pubSubClusterAddress;
  private final PubSubAdminAdapter pubSubAdminAdapterShared;

  public TopicManagerNew(TopicManagerContext topicManagerContext, String pubSubClusterAddress) {
    this.topicManagerContext = topicManagerContext;
    this.pubSubClusterAddress = pubSubClusterAddress;
    this.pubSubAdminAdapterShared = topicManagerContext.getPubSubAdminAdapterFactory()
        .create(
            topicManagerContext.getPubSubPropertiesSupplier().get(pubSubClusterAddress),
            topicManagerContext.getPubSubTopicRepository());
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

  public int getPartitionCount(PubSubTopic pubSubTopic) {
    // return pubSubAdminAdapterShared.getPartitionCount();
    return 0;
  }

  /**
   * The ''Cached' APIs are not expected to directly call pubSubAdmin and pubSubConsumer APIs.
   */
  public long getLatestOffsetCached(PubSubTopic pubSubTopic, int partitionId) {
    return -1;
  }

  public long getEarliestOffsetCached(PubSubTopic pubSubTopic, int partitionId) {
    return -1;
  }

  public long getEarliestOffsetCached(PubSubTopicPartition pubSubTopicPartition) {
    return -1;
  }

  public long getProducerTimestampOfLastDataMessageCached(PubSubTopicPartition pubSubTopicPartition) {
    return -1;
  }

  public boolean containsTopicCached(PubSubTopic pubSubTopic) {
    return false;
  }
}
