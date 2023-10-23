package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
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


public class TopicMetadataFetcherPool implements Closeable {
  /**
   * All metadata operations that are carried out by pubsub consumer should be done via TopicMetadataFetcher
   * obtained from this pool. PubSubConsumerAdapter is not thread-safe, so we need to use a pool to make sure
   * that only one thread is using a consumer at a time.
   */
  private final BlockingQueue<TopicMetadataFetcher> topicMetadataFetcherPool;
  private final List<Closeable> closeables = new ArrayList<>(2);

  /**
   * This thread pool is used to run TopicMetadataFetcher. We use a thread pool to make sure that we don't
   * create too many threads and not monopolize ForkJoinPool.commonPool().
   */
  private final ThreadPoolExecutor topicMetadataFetcherThreadPool;

  public TopicMetadataFetcherPool(
      String pubSubClusterAddress,
      TopicManagerContext topicManagerContext,
      Lazy<PubSubAdminAdapter> pubSubAdminAdapterShared) {
    topicMetadataFetcherPool = new LinkedBlockingQueue<>(topicManagerContext.getTopicMetadataFetcherPoolSize());

    TopicMetadataFetcherContext.Builder fetcherContextBuilder =
        new TopicMetadataFetcherContext.Builder().setPubSubClusterAddress(pubSubClusterAddress)
            .setPubSubAdminAdapterLazy(pubSubAdminAdapterShared)
            .setPubSubConsumerAdapterFactory(topicManagerContext.getPubSubConsumerAdapterFactory())
            .setPubSubProperties(topicManagerContext.getPubSubProperties(pubSubClusterAddress))
            .setPubSubMessageDeserializer(PubSubMessageDeserializer.getInstance());
    for (int i = 0; i < topicManagerContext.getTopicMetadataFetcherPoolSize(); i++) {
      fetcherContextBuilder.setFetcherId(i);
      TopicMetadataFetcher topicMetadataFetcher = new TopicMetadataFetcher(fetcherContextBuilder.build());
      closeables.add(topicMetadataFetcher);
      if (!topicMetadataFetcherPool.offer(topicMetadataFetcher)) {
        throw new VeniceException("Failed to initialize topicMetadataFetcherPool");
      }
    }

    topicMetadataFetcherThreadPool = new ThreadPoolExecutor(
        topicManagerContext.getTopicMetadataFetcherThreadPoolSize(),
        topicManagerContext.getTopicMetadataFetcherThreadPoolSize(),
        15L,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory("TopicMetadataFetcherThreadPool"));
    topicMetadataFetcherThreadPool.allowCoreThreadTimeOut(true);
  }

  // acquire a TopicMetadataFetcher from the pool
  private TopicMetadataFetcher acquire() {
    try {
      return topicMetadataFetcherPool.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while acquiring topicMetadataFetcher", e);
    }
  }

  // release a TopicMetadataFetcher to the pool
  private void release(TopicMetadataFetcher topicMetadataFetcher) {
    try {
      topicMetadataFetcherPool.put(topicMetadataFetcher);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while releasing topicMetadataFetcher", e);
    }
  }

  @Override
  public void close() throws IOException {
    topicMetadataFetcherThreadPool.shutdown();
    try {
      if (topicMetadataFetcherThreadPool.awaitTermination(5, TimeUnit.MILLISECONDS)) {
        topicMetadataFetcherThreadPool.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    for (Closeable closeable: closeables) {
      Utils.closeQuietlyWithErrorLogged(closeable);
    }
  }
}
