package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_KAFKA_OFFSET_API_TIMEOUT;

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
import com.linkedin.venice.utils.lazy.Lazy;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMaps;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TopicMetadataFetcher implements Closeable {

  // logger
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

  public TopicMetadataFetcher(
      String pubSubClusterAddress,
      TopicManagerContext topicManagerContext,
      Lazy<PubSubAdminAdapter> lazySharedPubSubAdminAdapter) {
    pubSubConsumerPool = new LinkedBlockingQueue<>(topicManagerContext.getTopicMetadataFetcherPoolSize());
    pubSubAdminAdapter = lazySharedPubSubAdminAdapter.get();

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

  private void isTopicPartitionValidWithRetries(PubSubTopicPartition pubSubTopicPartition) {
    if (pubSubTopicPartition.getPartitionNumber() < 0) {
      throw new IllegalArgumentException("Invalid partition number: " + pubSubTopicPartition.getPartitionNumber());
    }
    if (!pubSubAdminAdapter.containsTopicWithExpectationAndRetry(pubSubTopicPartition.getPubSubTopic(), 3, true)) {
      throw new PubSubTopicDoesNotExistException("Topic " + pubSubTopicPartition.getPubSubTopic() + " does not exist!");
    }
  }

  /*------------------------- PUBLIC APIs -------------------------*/

  // API#1: Get the latest offset for all partitions of a topic
  public Int2LongMap getTopicLatestOffsets(PubSubTopic topic) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      List<PubSubTopicPartitionInfo> partitionInfoList = pubSubConsumerAdapter.partitionsFor(topic);
      if (partitionInfoList == null || partitionInfoList.isEmpty()) {
        LOGGER.warn("Unexpected! Topic: {} has a null partition set, returning empty map for latest offsets", topic);
        return Int2LongMaps.EMPTY_MAP;
      }
      List<PubSubTopicPartition> topicPartitions = partitionInfoList.stream()
          .map(partitionInfo -> new PubSubTopicPartitionImpl(topic, partitionInfo.partition()))
          .collect(Collectors.toList());

      Map<PubSubTopicPartition, Long> offsetsByTopicPartitions =
          pubSubConsumerAdapter.endOffsets(topicPartitions, DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
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

  // API#2: Get information about all partitions of a topic
  public List<PubSubTopicPartitionInfo> getTopicPartitionInfo(PubSubTopic topic) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      return pubSubConsumerAdapter.partitionsFor(topic);
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  // API#3: Get the latest offset for all partitions of a topic
  public long getPartitionEarliestOffsetWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    isTopicPartitionValidWithRetries(pubSubTopicPartition);
    PubSubOpTimeoutException lastException = DUMMY_PUBSUB_OP_TIMEOUT_EXCEPTION;
    retries = Math.max(1, retries);
    int attempt = 0;
    while (attempt < retries) {
      PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
      try {
        Long offset = pubSubConsumerAdapter.beginningOffset(pubSubTopicPartition, DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
        if (offset == null) {
          throw new VeniceException(
              "offset result returned from beginningOffsets does not contain entry: " + pubSubTopicPartition);
        }
        return offset;
      } catch (PubSubOpTimeoutException e) {
        LOGGER.warn("Failed to get offset. Retries remaining: {}", retries - attempt, e);
        lastException = e;
        attempt++;
      } finally {
        releaseConsumer(pubSubConsumerAdapter);
      }
    }
    throw lastException;
  }

  CompletableFuture<Long> getPartitionEarliestOffsetWithRetriesAsync(
      PubSubTopicPartition pubSubTopicPartition,
      int retries) {
    return CompletableFuture
        .supplyAsync(() -> getPartitionEarliestOffsetWithRetries(pubSubTopicPartition, retries), threadPoolExecutor);
  }

  interface PubSubMetadataFetcher {
    long getPartitionLatestOffsetAndRetry(PubSubTopicPartition pubSubTopicPartition, int retries);

    long getPartitionEarliestOffsetAndRetry(PubSubTopicPartition pubSubTopicPartition, int retries);

    long getPartitionOffsetByTime(PubSubTopicPartition pubSubTopicPartition, long timestamp);

    /**
     * Get the producer timestamp of the last data message (non-control message) in the given topic partition. In other
     * words, if the last message in a topic partition is a control message, this method should keep looking at its previous
     * message(s) until it finds one that is not a control message and gets its producer timestamp.
     * @param pubSubTopicPartition
     * @param retries
     * @return producer timestamp
     */
    long getProducerTimestampOfLastDataRecord(PubSubTopicPartition pubSubTopicPartition, int retries);

    long getOffsetByTimeIfOutOfRange(PubSubTopicPartition pubSubTopicPartition, long timestamp);
  }
}
