package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;


/**
 * This class is used to fetch topic metadata from the pubsub cluster.
 *
 * It's responsibility of caller to ensure thread safety.
 *
 * Goals: to replace PartitionOffsetFetcher and CachedPubSubMetadataGetter
 *
 * This class is only intended to be used by the TopicManager.
 */
@NotThreadsafe
class TopicMetadataFetcher {
  private final PubSubAdminAdapter pubSubAdminAdapter;
  private final PubSubConsumerAdapter pubSubConsumerAdapter;
  private final String pubSubClusterAddress;

  TopicMetadataFetcher(
      PubSubAdminAdapter pubSubAdminAdapter,
      PubSubConsumerAdapter pubSubConsumerAdapter,
      String pubSubClusterAddress) {
    this.pubSubAdminAdapter = pubSubAdminAdapter;
    this.pubSubConsumerAdapter = pubSubConsumerAdapter;
    this.pubSubClusterAddress = pubSubClusterAddress;
  }

}
