package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A repository of {@link TopicManager} instances, each associated with a specific PubSub region and cluster.
 * This repository maintains one {@link TopicManager} for each unique PubSub bootstrap server address.
 * While not mandatory, it is expected that each Venice component will have one and only one instance of this class.
 */
public class TopicManagerRepository implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(TopicManagerRepository.class);
  private final Map<String, TopicManager> topicManagersMap = new VeniceConcurrentHashMap<>();
  private final Lazy<TopicManager> localTopicManager;
  private final String localPubSubClusterAddress;
  private final TopicManagerContext topicManagerContext;

  public TopicManagerRepository(TopicManagerContext topicManagerContext, String localPubSubClusterAddress) {
    this.localPubSubClusterAddress =
        Objects.requireNonNull(localPubSubClusterAddress, "localPubSubClusterAddress cannot be null");
    this.topicManagerContext = Objects.requireNonNull(topicManagerContext, "topicManagerContext cannot be null");
    this.localTopicManager = Lazy.of(
        () -> topicManagersMap.computeIfAbsent(
            this.localPubSubClusterAddress,
            k -> new TopicManager(topicManagerContext, this.localPubSubClusterAddress)));
  }

  /**
   * By default, return TopicManager for local PubSub cluster.
   */
  public TopicManager getTopicManager() {
    return localTopicManager.get();
  }

  public TopicManager getTopicManager(String pubSubClusterAddress) {
    return topicManagersMap
        .computeIfAbsent(pubSubClusterAddress, k -> new TopicManager(topicManagerContext, pubSubClusterAddress));
  }

  public List<TopicManager> getAllTopicManagers() {
    return new ArrayList<>(topicManagersMap.values());
  }

  // TODO: use async and concurrent close
  @Override
  public void close() {
    AtomicReference<Exception> lastException = new AtomicReference<>();
    topicManagersMap.entrySet().stream().forEach(entry -> {
      try {
        LOGGER.info("Closing TopicManager for PubSub cluster [" + entry.getKey() + "]");
        entry.getValue().close();
        LOGGER.info("Closed TopicManager for PubSub cluster [" + entry.getKey() + "]");
      } catch (Exception e) {
        LOGGER.error("Error when closing TopicManager for PubSub cluster [" + entry.getKey() + "]");
        lastException.set(e);
      }
    });
    if (lastException.get() != null) {
      throw new VeniceException(lastException.get());
    }
    LOGGER.info("All TopicManager closed.");
  }
}
