package com.linkedin.venice.pubsub.manager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The TopicMetadataCache class serves as a cache for storing metadata related to topics, including details
 * such as the latest offset, earliest offset, and topic existence. Cached values are set to expire after a
 * specified time-to-live duration. When a value is not present in the cache, it is synchronously fetched using
 * the topic metadata fetcher. If a cached value has expired, it is asynchronously retrieved, and the cached
 * value is updated upon completion of the asynchronous fetch. To avoid overwhelming metadata fetcher, only one
 * asynchronous request is issued at a time for a given key within the specific cache.
 */
class TopicMetadataCache implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(TopicMetadataCache.class);
  private static final int DEFAULT_MAX_RETRY = 3;

  private final TopicMetadataFetcher topicMetadataFetcher;
  private final Map<PubSubTopic, ValueAndExpiryTime<Boolean>> topicExistenceCache;
  private final Map<PubSubTopicPartition, ValueAndExpiryTime<Long>> latestOffsetCache;
  private final Map<PubSubTopicPartition, ValueAndExpiryTime<Long>> earliestOffsetCache;
  private final Map<PubSubTopicPartition, ValueAndExpiryTime<Long>> lastProducerTimestampCache;
  private final long ttlNs;

  TopicMetadataCache(TopicMetadataFetcher topicMetadataFetcher, String pubSubClusterAddress, long timeToLiveMs) {
    this.topicMetadataFetcher = Objects.requireNonNull(topicMetadataFetcher, "topicMetadataFetcher cannot be null");
    this.ttlNs = MILLISECONDS.toNanos(timeToLiveMs);
    this.topicExistenceCache = new VeniceConcurrentHashMap<>();
    this.latestOffsetCache = new VeniceConcurrentHashMap<>();
    this.earliestOffsetCache = new VeniceConcurrentHashMap<>();
    this.lastProducerTimestampCache = new VeniceConcurrentHashMap<>();
    LOGGER.info(
        "TopicMetadataCache initialized for pubSubClusterAddress: {} with ttlMs: {}",
        pubSubClusterAddress,
        timeToLiveMs);
  }

  long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    long now = System.nanoTime();
    ValueAndExpiryTime<Long> cachedValue;
    try {
      cachedValue = latestOffsetCache.computeIfAbsent(pubSubTopicPartition, k -> {
        long latestOffset = topicMetadataFetcher.getLatestOffsetWithRetries(pubSubTopicPartition, DEFAULT_MAX_RETRY);
        return new ValueAndExpiryTime<>(latestOffset, now + ttlNs);
      });
    } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
      LOGGER.error("Failed to get end offset for topic-partition: {}", pubSubTopicPartition, e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }

    if (cachedValue.getExpiryTimeNs() <= now && cachedValue.valueUpdateInProgress.compareAndSet(false, true)) {
      CompletableFuture<Long> endOffsetCompletableFuture =
          topicMetadataFetcher.getLatestOffsetWithRetriesAsync(pubSubTopicPartition, DEFAULT_MAX_RETRY);
      endOffsetCompletableFuture.whenComplete((latestOffset, throwable) -> {
        if (throwable != null) {
          latestOffsetCache.remove(pubSubTopicPartition);
        } else {
          latestOffsetCache
              .put(pubSubTopicPartition, new ValueAndExpiryTime<>(latestOffset, System.nanoTime() + ttlNs));
        }
      });
    }

    return cachedValue.getValue();
  }

  long getEarliestOffset(PubSubTopicPartition pubSubTopicPartition) {
    long now = System.nanoTime();
    ValueAndExpiryTime<Long> cachedValue;
    try {
      cachedValue = earliestOffsetCache.computeIfAbsent(pubSubTopicPartition, k -> {
        long earliestOffset =
            topicMetadataFetcher.getEarliestOffsetWithRetries(pubSubTopicPartition, DEFAULT_MAX_RETRY);
        return new ValueAndExpiryTime<>(earliestOffset, now + ttlNs);
      });
    } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
      LOGGER.error("Failed to get beginning offset for topic-partition: {}", pubSubTopicPartition, e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }

    // For a given key in the given cache, we will only issue one async request at the same time.
    if (cachedValue.getExpiryTimeNs() <= now && cachedValue.valueUpdateInProgress.compareAndSet(false, true)) {
      CompletableFuture<Long> beginningOffsetCompletableFuture =
          topicMetadataFetcher.getEarliestOffsetWithRetriesAsync(pubSubTopicPartition, DEFAULT_MAX_RETRY);
      beginningOffsetCompletableFuture.whenComplete((earliestOffset, throwable) -> {
        if (throwable != null) {
          earliestOffsetCache.remove(pubSubTopicPartition);
        } else {
          earliestOffsetCache
              .put(pubSubTopicPartition, new ValueAndExpiryTime<>(earliestOffset, System.nanoTime() + ttlNs));
        }
      });
    }

    return cachedValue.getValue();
  }

  boolean containsTopic(PubSubTopic pubSubTopic) {
    long now = System.nanoTime();
    ValueAndExpiryTime<Boolean> cachedValue = topicExistenceCache.computeIfAbsent(
        pubSubTopic,
        k -> new ValueAndExpiryTime<>(topicMetadataFetcher.containsTopic(pubSubTopic), now + ttlNs));
    if (cachedValue.getExpiryTimeNs() <= now && cachedValue.valueUpdateInProgress.compareAndSet(false, true)) {
      CompletableFuture<Boolean> containsTopicCompletableFuture = topicMetadataFetcher.containsTopicAsync(pubSubTopic);
      containsTopicCompletableFuture.whenComplete((containsTopic, throwable) -> {
        if (throwable != null) {
          topicExistenceCache.remove(pubSubTopic);
        } else {
          topicExistenceCache.put(pubSubTopic, new ValueAndExpiryTime<>(containsTopic, System.nanoTime() + ttlNs));
        }
      });
    }
    return cachedValue.getValue();
  }

  long getProducerTimestampOfLastDataMessage(PubSubTopicPartition pubSubTopicPartition) {
    long now = System.nanoTime();
    ValueAndExpiryTime<Long> cachedValue;
    try {
      cachedValue = lastProducerTimestampCache.computeIfAbsent(pubSubTopicPartition, k -> {
        long producerTimestamp = topicMetadataFetcher
            .getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, DEFAULT_MAX_RETRY);
        return new ValueAndExpiryTime<>(producerTimestamp, now + ttlNs);
      });
    } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
      LOGGER.error("Failed to get producer timestamp for topic-partition: {}", pubSubTopicPartition, e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }

    if (cachedValue.getExpiryTimeNs() <= now && cachedValue.valueUpdateInProgress.compareAndSet(false, true)) {
      CompletableFuture<Long> producerTimestampCompletableFuture = topicMetadataFetcher
          .getProducerTimestampOfLastDataMessageWithRetriesAsync(pubSubTopicPartition, DEFAULT_MAX_RETRY);
      producerTimestampCompletableFuture.whenComplete((producerTimestamp, throwable) -> {
        if (throwable != null) {
          lastProducerTimestampCache.remove(pubSubTopicPartition);
        } else {
          lastProducerTimestampCache
              .put(pubSubTopicPartition, new ValueAndExpiryTime<>(producerTimestamp, System.nanoTime() + ttlNs));
        }
      });
    }

    return cachedValue.getValue();
  }

  void invalidateKey(PubSubTopicPartition pubSubTopicPartition) {
    long startTime = System.currentTimeMillis();
    LOGGER.info("Invalidating cache for topic-partition: {}", pubSubTopicPartition);
    latestOffsetCache.remove(pubSubTopicPartition);
    earliestOffsetCache.remove(pubSubTopicPartition);
    lastProducerTimestampCache.remove(pubSubTopicPartition);
    LOGGER.info(
        "Invalidated cache for topic-partition: {} in {} ms",
        pubSubTopicPartition,
        System.currentTimeMillis() - startTime);
  }

  void invalidateKey(PubSubTopic pubSubTopic) {
    long startTime = System.currentTimeMillis();
    LOGGER.info("Invalidating cache for topic: {}", pubSubTopic);
    topicExistenceCache.remove(pubSubTopic);
    Set<PubSubTopicPartition> topicPartitions = new HashSet<>();
    for (PubSubTopicPartition pubSubTopicPartition: earliestOffsetCache.keySet()) {
      if (pubSubTopicPartition.getPubSubTopic().equals(pubSubTopic)) {
        topicPartitions.add(pubSubTopicPartition);
      }
    }
    earliestOffsetCache.keySet().removeAll(topicPartitions);
    topicPartitions.clear();
    for (PubSubTopicPartition pubSubTopicPartition: latestOffsetCache.keySet()) {
      if (pubSubTopicPartition.getPubSubTopic().equals(pubSubTopic)) {
        topicPartitions.add(pubSubTopicPartition);
      }
    }
    latestOffsetCache.keySet().removeAll(topicPartitions);
    topicPartitions.clear();
    for (PubSubTopicPartition pubSubTopicPartition: lastProducerTimestampCache.keySet()) {
      if (pubSubTopicPartition.getPubSubTopic().equals(pubSubTopic)) {
        topicPartitions.add(pubSubTopicPartition);
      }
    }
    lastProducerTimestampCache.keySet().removeAll(topicPartitions);
    LOGGER.info("Invalidated cache for topic: {} in {} ms", pubSubTopic, System.currentTimeMillis() - startTime);
  }

  @Override
  public void close() throws IOException {
    latestOffsetCache.clear();
    earliestOffsetCache.clear();
    lastProducerTimestampCache.clear();
    topicExistenceCache.clear();
  }

  /**
   * This class is used to store the value and expiry time of a cached value.
   */
  static class ValueAndExpiryTime<T> {
    private final T value;
    private final long expiryTimeNs;
    private final AtomicBoolean valueUpdateInProgress = new AtomicBoolean(false);

    ValueAndExpiryTime(T value, long expiryTimeNs) {
      this.value = value;
      this.expiryTimeNs = expiryTimeNs;
    }

    T getValue() {
      return value;
    }

    long getExpiryTimeNs() {
      return expiryTimeNs;
    }
  }
}
