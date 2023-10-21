package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_PUBSUB_OFFSET_API_TIMEOUT;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMaps;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class TopicMetadataFetcher implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(TopicMetadataFetcher.class);
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

  private String pubSubClusterAddress;

  public TopicMetadataFetcher(
      String pubSubClusterAddress,
      TopicManagerContext topicManagerContext,
      PubSubAdminAdapter pubSubAdminAdapter) {
    this.pubSubClusterAddress = pubSubClusterAddress;
    this.pubSubAdminAdapter = pubSubAdminAdapter;
    this.pubSubConsumerPool = new LinkedBlockingQueue<>(topicManagerContext.getTopicMetadataFetcherPoolSize());

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
    // consider removing this check if it's too expensive
    if (!pubSubAdminAdapter.containsTopic(pubSubTopicPartition.getPubSubTopic())) {
      throw new PubSubTopicDoesNotExistException(
          "Either topic: " + pubSubTopicPartition.getPubSubTopic() + " does not exist or partition: "
              + pubSubTopicPartition.getPartitionNumber() + " is invalid");
    }
  }

  @Override
  public void close() throws IOException {
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
    return -1;
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
}
