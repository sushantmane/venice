package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_PUBSUB_OFFSET_API_TIMEOUT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMaps;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class TopicMetadataFetcher implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(TopicMetadataFetcher.class);
  private static final int DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY = 3;
  public static final PubSubOpTimeoutException DUMMY_PUBSUB_OP_TIMEOUT_EXCEPTION =
      new PubSubOpTimeoutException("This exception should not be thrown");

  /**
   * Blocking queue is used to ensure single-threaded access to a consumer as PubSubConsumerAdapter
   * implementations are not guaranteed to be thread-safe.
   */
  private final BlockingQueue<PubSubConsumerAdapter> pubSubConsumerPool;
  private final List<Closeable> closeables = new ArrayList<>(2);
  private final ThreadPoolExecutor threadPoolExecutor;
  private final PubSubAdminAdapter pubSubAdminAdapter;

  /**
   * The following caches store metadata related to topics, including details such as the latest offset,
   * earliest offset, and topic existence. Cached values are set to expire after a specified time-to-live
   * duration. When a value is not present in the cache, it is synchronously. If a cached value has expired,
   * it is asynchronously retrieved, and the cache. value is updated upon completion of the asynchronous fetch.
   * To avoid overwhelming metadata fetcher, only one asynchronous request is issued at a time for a given
   * key within the specific cache.
   */
  private final Map<PubSubTopic, ValueAndExpiryTime<Boolean>> topicExistenceCache = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, ValueAndExpiryTime<Long>> latestOffsetCache = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, ValueAndExpiryTime<Long>> earliestOffsetCache =
      new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, ValueAndExpiryTime<Long>> lastProducerTimestampCache =
      new VeniceConcurrentHashMap<>();
  private final long cachedEntryTtlInNs;

  public TopicMetadataFetcher(
      String pubSubClusterAddress,
      TopicManagerContext topicManagerContext,
      PubSubAdminAdapter pubSubAdminAdapter) {
    this.pubSubAdminAdapter = pubSubAdminAdapter;
    this.pubSubConsumerPool = new LinkedBlockingQueue<>(topicManagerContext.getTopicMetadataFetcherPoolSize());
    this.cachedEntryTtlInNs = MILLISECONDS.toNanos(topicManagerContext.getTopicOffsetCheckIntervalMs());
    PubSubMessageDeserializer pubSubMessageDeserializer = PubSubMessageDeserializer.getInstance();
    for (int i = 0; i < topicManagerContext.getTopicMetadataFetcherPoolSize(); i++) {
      PubSubConsumerAdapter pubSubConsumerAdapter = topicManagerContext.getPubSubConsumerAdapterFactory()
          .create(
              topicManagerContext.getPubSubProperties(pubSubClusterAddress),
              false,
              pubSubMessageDeserializer,
              pubSubClusterAddress);

      closeables.add(pubSubConsumerAdapter);
      if (!pubSubConsumerPool.offer(pubSubConsumerAdapter)) {
        throw new VeniceException("Failed to initialize topicMetadataFetcherPool");
      }
    }

    threadPoolExecutor = new ThreadPoolExecutor(
        topicManagerContext.getTopicMetadataFetcherThreadPoolSize(),
        topicManagerContext.getTopicMetadataFetcherThreadPoolSize(),
        15L,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory("TopicMetadataFetcherThreadPool"));
    threadPoolExecutor.allowCoreThreadTimeOut(true);

    LOGGER.info(
        "Initialized TopicMetadataFetcher for pubSubClusterAddress: {} with consumer pool size: {} and thread pool size: {}",
        pubSubClusterAddress,
        topicManagerContext.getTopicMetadataFetcherPoolSize(),
        topicManagerContext.getTopicMetadataFetcherThreadPoolSize());
  }

  // acquire the consumer from the pool
  private PubSubConsumerAdapter acquireConsumer() {
    try {
      return pubSubConsumerPool.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while acquiring pubSubConsumerAdapter", e);
    }
  }

  // release the consumer back to the pool
  private void releaseConsumer(PubSubConsumerAdapter pubSubConsumerAdapter) {
    try {
      pubSubConsumerPool.put(pubSubConsumerAdapter);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while releasing pubSubConsumerAdapter", e);
    }
  }

  private void validateTopicPartition(PubSubTopicPartition pubSubTopicPartition) {
    if (pubSubTopicPartition.getPartitionNumber() < 0) {
      throw new IllegalArgumentException("Invalid partition number: " + pubSubTopicPartition.getPartitionNumber());
    }
    if (!containsTopicCached(pubSubTopicPartition.getPubSubTopic())) {
      throw new PubSubTopicDoesNotExistException(
          "Either topic: " + pubSubTopicPartition.getPubSubTopic() + " does not exist or partition: "
              + pubSubTopicPartition.getPartitionNumber() + " is invalid");
    }
  }

  @Override
  public void close() throws IOException {
    latestOffsetCache.clear();
    earliestOffsetCache.clear();
    lastProducerTimestampCache.clear();
    topicExistenceCache.clear();

    threadPoolExecutor.shutdown();
    try {
      if (threadPoolExecutor.awaitTermination(5, TimeUnit.MILLISECONDS)) {
        threadPoolExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    for (Closeable closeable: closeables) {
      Utils.closeQuietlyWithErrorLogged(closeable);
    }
  }

  // API: Check if a topic exists in the pubsub cluster
  boolean containsTopic(PubSubTopic topic) {
    return pubSubAdminAdapter.containsTopic(topic);
  }

  CompletableFuture<Boolean> containsTopicAsync(PubSubTopic topic) {
    return CompletableFuture.supplyAsync(() -> containsTopic(topic), threadPoolExecutor);
  }

  boolean containsTopicCached(PubSubTopic pubSubTopic) {
    long now = System.nanoTime();
    ValueAndExpiryTime<Boolean> cachedValue = topicExistenceCache.computeIfAbsent(
        pubSubTopic,
        k -> new ValueAndExpiryTime<>(containsTopic(pubSubTopic), now + cachedEntryTtlInNs));
    if (cachedValue.getExpiryTimeNs() <= now && cachedValue.tryAcquireUpdateLock()) {
      CompletableFuture<Boolean> containsTopicCompletableFuture = containsTopicAsync(pubSubTopic);
      containsTopicCompletableFuture.whenComplete((containsTopic, throwable) -> {
        if (throwable != null) {
          topicExistenceCache.remove(pubSubTopic);
        } else {
          topicExistenceCache
              .put(pubSubTopic, new ValueAndExpiryTime<>(containsTopic, System.nanoTime() + cachedEntryTtlInNs));
        }
      });
    }
    return cachedValue.getValue();
  }

  // API: Get the latest offsets for all partitions of a topic
  /**
   * Get the latest offsets for all partitions of a topic. This is a blocking call.
   * @param topic topic to get latest offsets for
   * @return a map of partition id to latest offset. If the topic does not exist, an empty map is returned.
   */
  Int2LongMap getTopicLatestOffsets(PubSubTopic topic) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      List<PubSubTopicPartitionInfo> partitionInfoList = pubSubConsumerAdapter.partitionsFor(topic);
      if (partitionInfoList == null || partitionInfoList.isEmpty()) {
        LOGGER.warn("Topic: {} may not exist or has no partitions. Returning empty map.", topic);
        return Int2LongMaps.EMPTY_MAP;
      }

      // convert to PubSubTopicPartition
      List<PubSubTopicPartition> topicPartitions = new ArrayList<>(partitionInfoList.size());
      for (PubSubTopicPartitionInfo partitionInfo: partitionInfoList) {
        topicPartitions.add(new PubSubTopicPartitionImpl(topic, partitionInfo.partition()));
      }

      Map<PubSubTopicPartition, Long> offsetsByTopicPartitions =
          pubSubConsumerAdapter.endOffsets(topicPartitions, DEFAULT_PUBSUB_OFFSET_API_TIMEOUT);
      Int2LongMap offsetsByTopicPartitionIds = new Int2LongOpenHashMap(offsetsByTopicPartitions.size());
      for (Map.Entry<PubSubTopicPartition, Long> offsetByTopicPartition: offsetsByTopicPartitions.entrySet()) {
        offsetsByTopicPartitionIds
            .put(offsetByTopicPartition.getKey().getPartitionNumber(), offsetByTopicPartition.getValue().longValue());
      }
      return offsetsByTopicPartitionIds;
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  // API: Get information about all partitions of a topic
  List<PubSubTopicPartitionInfo> getTopicPartitionInfo(PubSubTopic topic) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      return pubSubConsumerAdapter.partitionsFor(topic);
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  // API: Get the latest (end) offset for a topic partition
  /**
   * Retrieves the latest offset for the specified partition of a PubSub topic.
   *
   * @param pubSubTopicPartition The topic and partition number to query for the latest offset.
   * @return The latest offset for the specified partition.
   * @throws PubSubTopicDoesNotExistException If the topic does not exist.
   * @throws IllegalArgumentException If the partition number is negative.
   * @throws VeniceException If the offset returned by the consumer is null.
   *                         This could indicate a bug in the PubSubConsumerAdapter implementation.
   * @throws PubSubOpTimeoutException If the consumer times out. This could indicate that the topic does not exist
   *                         or the partition does not exist.
   */
  long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    validateTopicPartition(pubSubTopicPartition);
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      Map<PubSubTopicPartition, Long> offsetMap = pubSubConsumerAdapter
          .endOffsets(Collections.singletonList(pubSubTopicPartition), DEFAULT_PUBSUB_OFFSET_API_TIMEOUT);
      Long offset = offsetMap.get(pubSubTopicPartition);
      if (offset == null) {
        // This should never happen; if it does, it's a bug in the PubSubConsumerAdapter implementation
        throw new VeniceException("Got null as latest offset for: " + pubSubTopicPartition);
      }
      return offset;
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  long getLatestOffsetWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    VeniceException lastException = DUMMY_PUBSUB_OP_TIMEOUT_EXCEPTION;
    retries = Math.max(1, retries);
    int attempt = 0;
    while (attempt < retries) {
      try {
        return getLatestOffset(pubSubTopicPartition);
      } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
        lastException = e;
        attempt++;
        LOGGER.warn(
            "Failed to get the latest offset in attempt: {}/{} for: {}.",
            attempt,
            retries,
            pubSubTopicPartition,
            e);
      }
    }
    throw lastException;
  }

  CompletableFuture<Long> getLatestOffsetWithRetriesAsync(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return CompletableFuture
        .supplyAsync(() -> getLatestOffsetWithRetries(pubSubTopicPartition, retries), threadPoolExecutor);
  }

  long getLatestOffsetCached(PubSubTopicPartition pubSubTopicPartition) {
    long now = System.nanoTime();
    ValueAndExpiryTime<Long> cachedValue;
    try {
      cachedValue = latestOffsetCache.computeIfAbsent(pubSubTopicPartition, k -> {
        long latestOffset =
            getLatestOffsetWithRetries(pubSubTopicPartition, DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY);
        return new ValueAndExpiryTime<>(latestOffset, now + cachedEntryTtlInNs);
      });
    } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
      LOGGER.error("Failed to get end offset for topic-partition: {}", pubSubTopicPartition, e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }
    populateCacheWithLatestOffset(pubSubTopicPartition);
    return cachedValue.getValue();
  }

  // load the cache with the latest offset
  void populateCacheWithLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    long now = System.nanoTime();
    // if there is no entry in the cache or the entry has expired, we will try to populate the cache
    ValueAndExpiryTime<Long> cachedValue = latestOffsetCache.putIfAbsent(
        pubSubTopicPartition,
        new ValueAndExpiryTime<>((long) StatsErrorCode.LAG_MEASUREMENT_FAILURE.code, now + cachedEntryTtlInNs));
    if (cachedValue == null || (cachedValue.getExpiryTimeNs() <= now && cachedValue.tryAcquireUpdateLock())) {
      CompletableFuture<Long> future =
          getLatestOffsetWithRetriesAsync(pubSubTopicPartition, DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY);
      future.whenComplete((latestOffset, throwable) -> {
        if (throwable != null) {
          latestOffsetCache.remove(pubSubTopicPartition);
          return;
        }
        latestOffsetCache.get(pubSubTopicPartition).updateValue(latestOffset);
      });
    }
  }

  // API: Get the earliest (beginning) offset for a topic partition
  /**
   * Retrieves the earliest offset for the specified partition of a PubSub topic.
   *
   * @param pubSubTopicPartition The topic and partition number to query for the earliest offset.
   * @return The earliest offset for the specified partition.
   * @throws PubSubTopicDoesNotExistException If the topic does not exist.
   * @throws IllegalArgumentException If the partition number is negative.
   * @throws VeniceException If the offset returned by the consumer is null.
   *                         This could indicate a bug in the PubSubConsumerAdapter implementation.
   * @throws PubSubOpTimeoutException If the consumer times out. This could indicate that the topic does not exist
   *                         or the partition does not exist.
   */
  long getEarliestOffset(PubSubTopicPartition pubSubTopicPartition) {
    validateTopicPartition(pubSubTopicPartition);
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      Long offset = pubSubConsumerAdapter.beginningOffset(pubSubTopicPartition, DEFAULT_PUBSUB_OFFSET_API_TIMEOUT);
      if (offset == null) {
        // This should never happen; if it does, it's a bug in the PubSubConsumerAdapter implementation
        throw new VeniceException("Got null as earliest offset for: " + pubSubTopicPartition);
      }
      return offset;
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  long getEarliestOffsetWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    VeniceException lastException = DUMMY_PUBSUB_OP_TIMEOUT_EXCEPTION;
    retries = Math.max(1, retries);
    int attempt = 0;
    while (attempt < retries) {
      try {
        return getEarliestOffset(pubSubTopicPartition);
      } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
        lastException = e;
        attempt++;
        LOGGER.warn(
            "Failed to get the earliest offset in attempt: {}/{} for: {}.",
            attempt,
            retries,
            pubSubTopicPartition,
            e);
      }
    }
    throw lastException;
  }

  CompletableFuture<Long> getEarliestOffsetWithRetriesAsync(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return CompletableFuture
        .supplyAsync(() -> getEarliestOffsetWithRetries(pubSubTopicPartition, retries), threadPoolExecutor);
  }

  long getEarliestOffsetCached(PubSubTopicPartition pubSubTopicPartition) {
    long now = System.nanoTime();
    ValueAndExpiryTime<Long> cachedValue;
    try {
      cachedValue = earliestOffsetCache.computeIfAbsent(pubSubTopicPartition, k -> {
        long earliestOffset =
            getEarliestOffsetWithRetries(pubSubTopicPartition, DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY);
        return new ValueAndExpiryTime<>(earliestOffset, now + cachedEntryTtlInNs);
      });
    } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
      LOGGER.error("Failed to get beginning offset for topic-partition: {}", pubSubTopicPartition, e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }

    // For a given key in the given cache, we will only issue one async request at the same time.
    if (cachedValue.getExpiryTimeNs() <= now && cachedValue.tryAcquireUpdateLock()) {
      CompletableFuture<Long> beginningOffsetCompletableFuture =
          getEarliestOffsetWithRetriesAsync(pubSubTopicPartition, DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY);
      beginningOffsetCompletableFuture.whenComplete((earliestOffset, throwable) -> {
        if (throwable != null) {
          earliestOffsetCache.remove(pubSubTopicPartition);
        } else {
          earliestOffsetCache.put(
              pubSubTopicPartition,
              new ValueAndExpiryTime<>(earliestOffset, System.nanoTime() + cachedEntryTtlInNs));
        }
      });
    }

    return cachedValue.getValue();
  }

  // API: Get the offset for a given timestamp
  long getOffsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    // We start by retrieving the latest offset. If the provided timestamp is out of range,
    // we return the latest offset. This ensures that we don't miss any records produced
    // after the 'offsetForTime' call when the latest offset is obtained after timestamp checking.
    long latestOffset = getLatestOffset(pubSubTopicPartition);

    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      Long result =
          pubSubConsumerAdapter.offsetForTime(pubSubTopicPartition, timestamp, DEFAULT_PUBSUB_OFFSET_API_TIMEOUT);
      if (result == null) {
        // when offset is null, it means the given timestamp is either
        // out of range or the topic does not have any message
        return latestOffset;
      }
      return result;
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  long getOffsetForTimeWithRetries(PubSubTopicPartition pubSubTopicPartition, long timestamp, int retries) {
    validateTopicPartition(pubSubTopicPartition);
    VeniceException lastException = DUMMY_PUBSUB_OP_TIMEOUT_EXCEPTION;
    retries = Math.max(1, retries);
    int attempt = 0;
    while (attempt < retries) {
      try {
        return getOffsetForTime(pubSubTopicPartition, timestamp);
      } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
        lastException = e;
        attempt++;
        LOGGER.warn(
            "Failed to get the offset for time in attempt: {}/{} for: {}.",
            attempt,
            retries,
            pubSubTopicPartition,
            e);
      }
    }
    throw lastException;
  }

  // API: Get the producer timestamp of the last data message
  long getProducerTimestampOfLastDataMessage(PubSubTopicPartition pubSubTopicPartition) {
    int fetchSize = 10;
    int totalAttempts = 2;
    int fetchedRecordsCount;
    do {
      List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> lastConsumedRecords =
          consumeLatestRecords(pubSubTopicPartition, fetchSize);
      // if there are no records in this topic partition, return a special timestamp
      if (lastConsumedRecords.isEmpty()) {
        return PubSubConstants.NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION;
      }

      fetchedRecordsCount = lastConsumedRecords.size();
      // iterate in reverse order to find the first data message (not control message) from the end
      for (int i = lastConsumedRecords.size() - 1; i >= 0; i--) {
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = lastConsumedRecords.get(i);
        if (!record.getKey().isControlMessage()) {
          // note that the timestamp is the producer timestamp and not the pubsub message (broker) timestamp
          return record.getValue().producerMetadata.messageTimestamp;
        }
      }
      fetchSize = 50;
    } while (--totalAttempts > 0);

    LOGGER.warn(
        "Failed to find latest data message producer timestamp in topic-partition: {}. Consumed {} records from the end.",
        pubSubTopicPartition,
        fetchedRecordsCount);
    throw new VeniceException(
        "Failed to find latest data message producer timestamp in topic-partition: " + pubSubTopicPartition
            + ". Consumed " + fetchedRecordsCount + " records from the end.");
  }

  long getProducerTimestampOfLastDataMessageWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    VeniceException lastException = DUMMY_PUBSUB_OP_TIMEOUT_EXCEPTION;
    retries = Math.max(1, retries);
    int attempt = 0;
    while (attempt < retries) {
      try {
        return getProducerTimestampOfLastDataMessage(pubSubTopicPartition);
      } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
        lastException = e;
        attempt++;
        LOGGER.warn(
            "Failed to get the producer timestamp of last data message in attempt: {}/{} for: {}.",
            attempt,
            retries,
            pubSubTopicPartition,
            e);
      }
    }
    throw lastException;
  }

  CompletableFuture<Long> getProducerTimestampOfLastDataMessageWithRetriesAsync(
      PubSubTopicPartition pubSubTopicPartition,
      int retries) {
    return CompletableFuture.supplyAsync(
        () -> getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, retries),
        threadPoolExecutor);
  }

  long getProducerTimestampOfLastDataMessageCached(PubSubTopicPartition pubSubTopicPartition) {
    long now = System.nanoTime();
    ValueAndExpiryTime<Long> cachedValue;
    try {
      cachedValue = lastProducerTimestampCache.computeIfAbsent(pubSubTopicPartition, k -> {
        long producerTimestamp = getProducerTimestampOfLastDataMessageWithRetries(
            pubSubTopicPartition,
            DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY);
        return new ValueAndExpiryTime<>(producerTimestamp, now + cachedEntryTtlInNs);
      });
    } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
      LOGGER.error("Failed to get producer timestamp for topic-partition: {}", pubSubTopicPartition, e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }

    if (cachedValue.getExpiryTimeNs() <= now && cachedValue.tryAcquireUpdateLock()) {
      CompletableFuture<Long> producerTimestampCompletableFuture =
          getProducerTimestampOfLastDataMessageWithRetriesAsync(
              pubSubTopicPartition,
              DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY);
      producerTimestampCompletableFuture.whenComplete((producerTimestamp, throwable) -> {
        if (throwable != null) {
          lastProducerTimestampCache.remove(pubSubTopicPartition);
        } else {
          lastProducerTimestampCache.put(
              pubSubTopicPartition,
              new ValueAndExpiryTime<>(producerTimestamp, System.nanoTime() + cachedEntryTtlInNs));
        }
      });
    }

    return cachedValue.getValue();
  }

  /**
   * This method retrieves last {@code lastRecordsCount} records from a topic partition and there are 4 steps below.
   *  1. Find the current end offset N
   *  2. Seek back {@code lastRecordsCount} records from the end offset N
   *  3. Keep consuming records until the last consumed offset is greater than or equal to N
   *  4. Return all consumed records
   *
   * There are 2 things to note:
   *   1. When this method returns, these returned records are not necessarily the "last" records because after step 2,
   *      there could be more records produced to this topic partition and this method only consume records until the end
   *      offset retrieved at the above step 2.
   *
   *   2. This method might return more than {@code lastRecordsCount} records since the consumer poll method gets a batch
   *      of consumer records each time and the batch size is arbitrary.
   */
  private List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumeLatestRecords(
      PubSubTopicPartition pubSubTopicPartition,
      int lastRecordsCount) {
    if (lastRecordsCount < 1) {
      throw new IllegalArgumentException(
          "Last record count must be greater than or equal to 1. Got: " + lastRecordsCount);
    }
    validateTopicPartition(pubSubTopicPartition);
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    boolean subscribed = false;
    try {
      // find the end offset
      Map<PubSubTopicPartition, Long> offsetMap = pubSubConsumerAdapter
          .endOffsets(Collections.singletonList(pubSubTopicPartition), DEFAULT_PUBSUB_OFFSET_API_TIMEOUT);
      long latestOffset = offsetMap.get(pubSubTopicPartition);
      if (latestOffset <= 0) {
        return Collections.emptyList(); // no records in this topic partition
      }

      // find the beginning offset
      long earliestOffset =
          pubSubConsumerAdapter.beginningOffset(pubSubTopicPartition, DEFAULT_PUBSUB_OFFSET_API_TIMEOUT);
      if (earliestOffset == latestOffset) {
        return Collections.emptyList(); // no records in this topic partition
      }

      // consume latest records
      long consumerFromOffset = Math.max(latestOffset - lastRecordsCount, earliestOffset);
      pubSubConsumerAdapter.subscribe(pubSubTopicPartition, consumerFromOffset - 1);
      subscribed = true;
      List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> allConsumedRecords = new ArrayList<>(lastRecordsCount);

      // Keep consuming records from that topic-partition until the last consumed record's
      // offset is greater or equal to the partition end offset retrieved before.
      do {
        List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumedRecordsBatch = Collections.emptyList();
        int attempt = 1;
        while (consumedRecordsBatch.isEmpty()
            && attempt <= PubSubConstants.PUBSUB_CONSUMER_POLLING_FOR_METADATA_RETRY_MAX_ATTEMPT) {
          LOGGER.info(
              "Polling records from topic partition {} with start offset: {}. Attempt: {}/{}",
              pubSubTopicPartition,
              consumerFromOffset,
              attempt,
              PubSubConstants.PUBSUB_CONSUMER_POLLING_FOR_METADATA_RETRY_MAX_ATTEMPT);
          consumedRecordsBatch =
              pubSubConsumerAdapter.poll(Math.max(5000, DEFAULT_PUBSUB_OFFSET_API_TIMEOUT.toMillis()))
                  .get(pubSubTopicPartition);
          attempt++;
        }

        // If batch is still empty after retries, give up.
        if (consumedRecordsBatch.isEmpty()) {
          LOGGER.error(
              "Failed to get the last record from topic-partition: {} after {} attempts",
              pubSubTopicPartition,
              PubSubConstants.PUBSUB_CONSUMER_POLLING_FOR_METADATA_RETRY_MAX_ATTEMPT);
          throw new VeniceException(
              "Failed to get the last record from topic-partition: " + pubSubTopicPartition + " after "
                  + PubSubConstants.PUBSUB_CONSUMER_POLLING_FOR_METADATA_RETRY_MAX_ATTEMPT + " attempts");
        }
        allConsumedRecords.addAll(consumedRecordsBatch);
      } while (allConsumedRecords.get(allConsumedRecords.size() - 1).getOffset() + 1 < latestOffset);

      return allConsumedRecords;
    } finally {
      if (subscribed) {
        pubSubConsumerAdapter.unSubscribe(pubSubTopicPartition);
      }
      releaseConsumer(pubSubConsumerAdapter);
    }
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

  CompletableFuture<Void> invalidateKeyAsync(PubSubTopic pubSubTopic) {
    return CompletableFuture.runAsync(() -> invalidateKey(pubSubTopic));
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

  /**
   * This class is used to store the value and expiry time of a cached value.
   */
  static class ValueAndExpiryTime<T> {
    private T value;
    private long expiryTimeNs;
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

    boolean tryAcquireUpdateLock() {
      return valueUpdateInProgress.compareAndSet(false, true);
    }

    void updateValue(T value) {
      this.value = value;
      this.expiryTimeNs = System.nanoTime();
      this.valueUpdateInProgress.set(false);
    }
  }
}
