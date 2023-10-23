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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TopicMetadataFetcher implements Closeable {

  // logger
  private static final Logger LOGGER = LogManager.getLogger(TopicMetadataFetcher.class);

  /**
   * Blocking queue is used to ensure single-threaded access to the consumer as they are not thread-safe.
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

  public long getPartitionEarliestOffsetAndRetry(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return getEndOffset(pubSubTopicPartition, retries, this::getEarliestOffset);
  }

  private long getEndOffset(
      PubSubTopicPartition pubSubTopicPartition,
      int retries,
      Function<PubSubTopicPartition, Long> offsetSupplier) {
    if (retries < 1) {
      throw new IllegalArgumentException("Invalid retries. Got: " + retries);
    }
    int attempt = 0;
    PubSubOpTimeoutException lastException = new PubSubOpTimeoutException("This exception should not be thrown");
    while (attempt < retries) {
      try {
        return offsetSupplier.apply(pubSubTopicPartition);
      } catch (PubSubOpTimeoutException e) { // topic and partition is listed in the exception object
        LOGGER.warn("Failed to get offset. Retries remaining: {}", retries - attempt, e);
        lastException = e;
        attempt++;
      }
    }
    throw lastException;
  }

  /**
   * @return the beginning offset of a topic-partition.
   */
  private long getEarliestOffset(PubSubTopicPartition pubSubTopicPartition) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      Long offset = pubSubConsumerAdapter.beginningOffset(pubSubTopicPartition, DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
      if (offset == null) {
        throw new VeniceException(
            "offset result returned from beginningOffsets does not contain entry: " + pubSubTopicPartition);
      }
      return offset;
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
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

  interface PubSubMetadataFetcher {
    Int2LongMap getTopicLatestOffsets(PubSubTopic topic); //done

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

    List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic);

    long getOffsetByTimeIfOutOfRange(PubSubTopicPartition pubSubTopicPartition, long timestamp);
  }
}
