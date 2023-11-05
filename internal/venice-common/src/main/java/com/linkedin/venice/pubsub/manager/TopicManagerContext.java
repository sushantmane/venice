package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_PUBSUB_OPERATION_TIMEOUT_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS;

import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Objects;


/**
 * A context object that contains all the dependencies needed by {@link TopicManager}.
 */
public class TopicManagerContext {
  private final PubSubAdminAdapterFactory<PubSubAdminAdapter> pubSubAdminAdapterFactory;
  private final PubSubConsumerAdapterFactory<PubSubConsumerAdapter> pubSubConsumerAdapterFactory;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final MetricsRepository metricsRepository;
  private final PubSubPropertiesSupplier pubSubPropertiesSupplier;
  private final long pubSubOperationTimeoutMs;
  private final long topicDeletionStatusPollIntervalMs;
  private final long topicMinLogCompactionLagMs;
  private final long topicOffsetCheckIntervalMs;
  private final int topicMetadataFetcherPoolSize;
  private final int topicMetadataFetcherThreadPoolSize;

  private TopicManagerContext(Builder builder) {
    this.pubSubOperationTimeoutMs = builder.pubSubOperationTimeoutMs;
    this.topicDeletionStatusPollIntervalMs = builder.topicDeletionStatusPollIntervalMs;
    this.topicMinLogCompactionLagMs = builder.topicMinLogCompactionLagMs;
    this.pubSubAdminAdapterFactory = builder.pubSubAdminAdapterFactory;
    this.pubSubConsumerAdapterFactory = builder.pubSubConsumerAdapterFactory;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.metricsRepository = builder.metricsRepository;
    this.pubSubPropertiesSupplier = builder.pubSubPropertiesSupplier;
    this.topicOffsetCheckIntervalMs = builder.topicOffsetCheckIntervalMs;
    this.topicMetadataFetcherPoolSize = builder.topicMetadataFetcherPoolSize;
    this.topicMetadataFetcherThreadPoolSize = builder.topicMetadataFetcherThreadPoolSize;
  }

  public long getPubSubOperationTimeoutMs() {
    return pubSubOperationTimeoutMs;
  }

  public long getTopicDeletionStatusPollIntervalMs() {
    return topicDeletionStatusPollIntervalMs;
  }

  public long getTopicMinLogCompactionLagMs() {
    return topicMinLogCompactionLagMs;
  }

  public PubSubAdminAdapterFactory<PubSubAdminAdapter> getPubSubAdminAdapterFactory() {
    return pubSubAdminAdapterFactory;
  }

  public PubSubConsumerAdapterFactory<PubSubConsumerAdapter> getPubSubConsumerAdapterFactory() {
    return pubSubConsumerAdapterFactory;
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public PubSubPropertiesSupplier getPubSubPropertiesSupplier() {
    return pubSubPropertiesSupplier;
  }

  public VeniceProperties getPubSubProperties(String pubSubBootstrapServers) {
    return pubSubPropertiesSupplier.get(pubSubBootstrapServers);
  }

  public long getTopicOffsetCheckIntervalMs() {
    return topicOffsetCheckIntervalMs;
  }

  public int getTopicMetadataFetcherPoolSize() {
    return topicMetadataFetcherPoolSize;
  }

  public int getTopicMetadataFetcherThreadPoolSize() {
    return topicMetadataFetcherThreadPoolSize;
  }

  public interface PubSubPropertiesSupplier {
    VeniceProperties get(String pubSubBootstrapServers);
  }

  @Override
  public String toString() {
    return "TopicManagerContext{" + ", pubSubOperationTimeoutMs=" + pubSubOperationTimeoutMs
        + ", topicDeletionStatusPollIntervalMs=" + topicDeletionStatusPollIntervalMs + ", topicMinLogCompactionLagMs="
        + topicMinLogCompactionLagMs + ", topicOffsetCheckIntervalMs=" + topicOffsetCheckIntervalMs
        + ", topicMetadataFetcherPoolSize=" + topicMetadataFetcherPoolSize + ", topicMetadataFetcherThreadPoolSize="
        + topicMetadataFetcherThreadPoolSize + ", pubSubAdminAdapterFactory="
        + pubSubAdminAdapterFactory.getClass().getSimpleName() + ", pubSubConsumerAdapterFactory="
        + pubSubConsumerAdapterFactory.getClass().getSimpleName() + '}';
  }

  public static class Builder {
    private PubSubAdminAdapterFactory<PubSubAdminAdapter> pubSubAdminAdapterFactory;
    private PubSubConsumerAdapterFactory<PubSubConsumerAdapter> pubSubConsumerAdapterFactory;
    private PubSubTopicRepository pubSubTopicRepository;
    private MetricsRepository metricsRepository;
    private PubSubPropertiesSupplier pubSubPropertiesSupplier;
    private long pubSubOperationTimeoutMs = DEFAULT_PUBSUB_OPERATION_TIMEOUT_MS;
    private long topicDeletionStatusPollIntervalMs = DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS;
    private long topicMinLogCompactionLagMs = DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS;
    private long topicOffsetCheckIntervalMs = 60_000L; // 1 minute
    private int topicMetadataFetcherPoolSize = 2; // 2 fetchers per broker
    private int topicMetadataFetcherThreadPoolSize =
        Math.min(Runtime.getRuntime().availableProcessors() / 2, topicMetadataFetcherPoolSize);

    public Builder setPubSubOperationTimeoutMs(long pubSubOperationTimeoutMs) {
      this.pubSubOperationTimeoutMs = pubSubOperationTimeoutMs;
      return this;
    }

    public Builder setTopicDeletionStatusPollIntervalMs(long topicDeletionStatusPollIntervalMs) {
      this.topicDeletionStatusPollIntervalMs = topicDeletionStatusPollIntervalMs;
      return this;
    }

    public Builder setTopicMinLogCompactionLagMs(long topicMinLogCompactionLagMs) {
      this.topicMinLogCompactionLagMs = topicMinLogCompactionLagMs;
      return this;
    }

    public Builder setPubSubAdminAdapterFactory(
        PubSubAdminAdapterFactory<PubSubAdminAdapter> pubSubAdminAdapterFactory) {
      this.pubSubAdminAdapterFactory =
          Objects.requireNonNull(pubSubAdminAdapterFactory, "pubSubAdminAdapterFactory cannot be null");
      return this;
    }

    public Builder setPubSubConsumerAdapterFactory(
        PubSubConsumerAdapterFactory<PubSubConsumerAdapter> pubSubConsumerAdapterFactory) {
      this.pubSubConsumerAdapterFactory =
          Objects.requireNonNull(pubSubConsumerAdapterFactory, "pubSubConsumerAdapterFactory cannot be null");
      return this;
    }

    public Builder setPubSubTopicRepository(PubSubTopicRepository pubSubTopicRepository) {
      this.pubSubTopicRepository =
          Objects.requireNonNull(pubSubTopicRepository, "pubSubTopicRepository cannot be null");
      return this;
    }

    public Builder setMetricsRepository(MetricsRepository metricsRepository) {
      this.metricsRepository = metricsRepository;
      return this;
    }

    public Builder setPubSubProperties(PubSubPropertiesSupplier pubSubPropertiesSupplier) {
      this.pubSubPropertiesSupplier =
          Objects.requireNonNull(pubSubPropertiesSupplier, "pubSubPropertiesSupplier cannot be null");
      return this;
    }

    public Builder setTopicOffsetCheckIntervalMs(long topicOffsetCheckIntervalMs) {
      this.topicOffsetCheckIntervalMs = topicOffsetCheckIntervalMs;
      return this;
    }

    public Builder setTopicMetadataFetcherPoolSize(int topicMetadataFetcherPoolSize) {
      if (topicMetadataFetcherPoolSize < 1) {
        throw new IllegalArgumentException("topicMetadataFetcherPoolSize must be at least 1");
      }
      this.topicMetadataFetcherPoolSize = topicMetadataFetcherPoolSize;
      return this;
    }

    public Builder setTopicMetadataFetcherThreadPoolSize(int topicMetadataFetcherThreadPoolSize) {
      if (topicMetadataFetcherThreadPoolSize < 1) {
        throw new IllegalArgumentException("topicMetadataFetcherThreadPoolSize must be at least 1");
      }
      this.topicMetadataFetcherThreadPoolSize = topicMetadataFetcherThreadPoolSize;
      return this;
    }

    public TopicManagerContext build() {
      return new TopicManagerContext(this);
    }
  }
}
