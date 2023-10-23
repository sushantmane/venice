package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
  private static final Logger LOGGER = LogManager.getLogger(TopicMetadataFetcher.class);

  private final String pubSubClusterAddress;
  private final Lazy<PubSubAdminAdapter> pubSubAdminAdapterLazy;
  private final Lazy<PubSubConsumerAdapter> pubSubConsumerAdapterLazy;

  TopicMetadataFetcher(TopicMetadataFetcherContext fetcherContext) {
    this.pubSubClusterAddress = fetcherContext.getPubSubClusterAddress();
    this.pubSubAdminAdapterLazy = fetcherContext.getPubSubAdminAdapterLazy();
    this.pubSubConsumerAdapterLazy = Lazy.of(
        () -> fetcherContext.getPubSubConsumerAdapterFactory()
            .create(
                fetcherContext.getPubSubProperties(pubSubClusterAddress),
                false,
                fetcherContext.getPubSubMessageDeserializer(),
                pubSubClusterAddress));
    ;
  }

}
