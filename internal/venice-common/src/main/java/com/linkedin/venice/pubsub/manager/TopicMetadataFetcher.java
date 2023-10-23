package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_KAFKA_OFFSET_API_TIMEOUT;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
}
