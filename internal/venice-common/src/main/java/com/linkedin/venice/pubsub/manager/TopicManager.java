package com.linkedin.venice.pubsub.manager;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubInstrumentedAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicExistsException;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * TopicManager is a wrapper around {@link PubSubAdminAdapter} and {@link PubSubConsumerAdapter}. It provides
 * a set of APIs to manage topics, including creating topics, updating topic retention, fetching topic metadata, etc.
 * <p>
 * This class contains one global {@link PubSubConsumerAdapter}, which is not thread-safe, so when you add new functions,
 * which is using this global consumer, please add 'synchronized' keyword, otherwise this {@link TopicManager}
 * won't be thread-safe, and Kafka consumer will report the following error when multiple threads are trying to
 * use the same consumer: PubSubConsumerAdapter is not safe for multithreaded access.
 */
public class TopicManager implements Closeable {
  private final Logger logger;
  private final String pubSubClusterAddress;
  private final TopicManagerContext topicManagerContext;
  private final PubSubAdminAdapter pubSubAdminAdapter;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final MetricsRepository metricsRepository;

  // A thread-safe partition offset fetcher.
  private final TopicMetadataFetcher topicMetadataFetcher;

  // TODO: Consider moving this cache to TopicMetadataFetcher
  // It's expensive to grab the topic config over and over again, and it changes infrequently.
  // So we temporarily cache queried configs.
  Cache<PubSubTopic, PubSubTopicConfiguration> topicConfigCache =
      Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

  public TopicManager(String pubSubClusterAddress, TopicManagerContext tmContext) {
    this.logger = LogManager.getLogger(
        TopicManager.class.getSimpleName() + " [" + Utils.getSanitizedStringForLogger(pubSubClusterAddress) + "]");
    this.pubSubClusterAddress = Objects.requireNonNull(pubSubClusterAddress, "pubSubClusterAddress cannot be null");
    this.topicManagerContext = tmContext;
    this.pubSubTopicRepository = tmContext.getPubSubTopicRepository();
    this.metricsRepository = tmContext.getMetricsRepository();
    this.pubSubAdminAdapter = createdPubSubAdminAdapter();
    this.topicMetadataFetcher = new TopicMetadataFetcher(pubSubClusterAddress, tmContext, pubSubAdminAdapter);
    this.logger.info(
        "Created topic manager for the pubsub cluster address: {} with context: {}",
        pubSubClusterAddress,
        tmContext);
  }

  // For testing only
  TopicManager(
      String pubSubClusterAddress,
      TopicManagerContext topicManagerContext,
      PubSubTopicRepository pubSubTopicRepository,
      MetricsRepository metricsRepository,
      PubSubAdminAdapter pubSubAdminAdapter,
      TopicMetadataFetcher topicMetadataFetcher) {
    this.logger = LogManager.getLogger(
        TopicManager.class.getSimpleName() + " [" + Utils.getSanitizedStringForLogger(pubSubClusterAddress) + "]");
    this.pubSubClusterAddress = Objects.requireNonNull(pubSubClusterAddress, "pubSubClusterAddress cannot be null");
    this.topicManagerContext = topicManagerContext;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.metricsRepository = metricsRepository;
    this.pubSubAdminAdapter = pubSubAdminAdapter;
    this.topicMetadataFetcher = topicMetadataFetcher;
    this.logger.info(
        "Created topic manager for the pubsub cluster address: {} with context: {}",
        pubSubClusterAddress,
        topicManagerContext);
  }

  private PubSubAdminAdapter createdPubSubAdminAdapter() {
    logger.info("Creating pubsub admin client for bootstrap servers: {}", pubSubClusterAddress);
    PubSubAdminAdapter pubSubAdminAdapter = topicManagerContext.getPubSubAdminAdapterFactory()
        .create(topicManagerContext.getPubSubProperties(pubSubClusterAddress), pubSubTopicRepository);
    if (metricsRepository != null) {
      String statsName = "TopicManagerPubSubAdminClient" + "_" + pubSubClusterAddress;
      pubSubAdminAdapter = new PubSubInstrumentedAdminAdapter(pubSubAdminAdapter, metricsRepository, statsName);
    }
    logger.info(
        "Created pubsub admin client of type: {} for bootstrap servers: {}",
        pubSubAdminAdapter.getClassName(),
        pubSubClusterAddress);
    return pubSubAdminAdapter;
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value PubSubConstants#DEFAULT_PUBSUB_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @see {@link #createTopic(PubSubTopic, int, int, boolean, boolean, Optional)}
   */
  public void createTopic(PubSubTopic topicName, int numPartitions, int replication, boolean eternal) {
    createTopic(topicName, numPartitions, replication, eternal, false, Optional.empty(), false);
  }

  public void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      boolean eternal,
      boolean logCompaction,
      Optional<Integer> minIsr) {
    createTopic(topicName, numPartitions, replication, eternal, logCompaction, minIsr, true);
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value PubSubConstants#DEFAULT_PUBSUB_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @param topicName Name for the new topic
   * @param numPartitions number of partitions
   * @param replication replication factor
   * @param eternal if true, the topic will have "infinite" (~250 mil years) retention
   *                if false, its retention will be set to {@link PubSubConstants#DEFAULT_TOPIC_RETENTION_POLICY_MS} by default
   * @param logCompaction whether to enable log compaction on the topic
   * @param minIsr if present, will apply the specified min.isr to this topic,
   *               if absent, Kafka cluster defaults will be used
   * @param useFastKafkaOperationTimeout if false, normal kafka operation timeout will be used,
   *                            if true, a much shorter timeout will be used to make topic creation non-blocking.
   */
  public void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      boolean eternal,
      boolean logCompaction,
      Optional<Integer> minIsr,
      boolean useFastKafkaOperationTimeout) {
    long retentionTimeMs;
    if (eternal) {
      retentionTimeMs = PubSubConstants.ETERNAL_TOPIC_RETENTION_POLICY_MS;
    } else {
      retentionTimeMs = PubSubConstants.DEFAULT_TOPIC_RETENTION_POLICY_MS;
    }
    createTopic(
        topicName,
        numPartitions,
        replication,
        retentionTimeMs,
        logCompaction,
        minIsr,
        useFastKafkaOperationTimeout);
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value PubSubConstants#DEFAULT_PUBSUB_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @param topicName Name for the new topic
   * @param numPartitions number of partitions
   * @param replication replication factor
   * @param retentionTimeMs Retention time, in ms, for the topic
   * @param logCompaction whether to enable log compaction on the topic
   * @param minIsr if present, will apply the specified min.isr to this topic,
   *               if absent, Kafka cluster defaults will be used
   * @param useFastKafkaOperationTimeout if false, normal kafka operation timeout will be used,
   *                            if true, a much shorter timeout will be used to make topic creation non-blocking.
   */
  public void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      long retentionTimeMs,
      boolean logCompaction,
      Optional<Integer> minIsr,
      boolean useFastKafkaOperationTimeout) {

    long startTime = System.currentTimeMillis();
    long deadlineMs = startTime + (useFastKafkaOperationTimeout
        ? PubSubConstants.FAST_KAFKA_OPERATION_TIMEOUT_MS
        : topicManagerContext.getPubSubOperationTimeoutMs());
    PubSubTopicConfiguration pubSubTopicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionTimeMs),
        logCompaction,
        minIsr,
        topicManagerContext.getTopicMinLogCompactionLagMs(),
        Optional.empty());
    logger.info(
        "Creating topic: {} partitions: {} replication: {}, configuration: {}",
        topicName,
        numPartitions,
        replication,
        pubSubTopicConfiguration);

    try {
      RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> pubSubAdminAdapter.createTopic(topicName, numPartitions, replication, pubSubTopicConfiguration),
          10,
          Duration.ofMillis(200),
          Duration.ofSeconds(1),
          Duration.ofMillis(
              useFastKafkaOperationTimeout
                  ? PubSubConstants.FAST_KAFKA_OPERATION_TIMEOUT_MS
                  : topicManagerContext.getPubSubOperationTimeoutMs()),
          PubSubConstants.CREATE_TOPIC_RETRIABLE_EXCEPTIONS);
    } catch (Exception e) {
      if (ExceptionUtils.recursiveClassEquals(e, PubSubTopicExistsException.class)) {
        logger.info("Topic: {} already exists, will update retention policy.", topicName);
        waitUntilTopicCreated(topicName, numPartitions, deadlineMs);
        updateTopicRetention(topicName, retentionTimeMs);
        logger.info("Updated retention policy to be {}ms for topic: {}", retentionTimeMs, topicName);
        return;
      } else {
        throw new PubSubOpTimeoutException(
            "Timeout while creating topic: " + topicName + ". Topic still does not exist after "
                + (deadlineMs - startTime) + "ms.",
            e);
      }
    }
    waitUntilTopicCreated(topicName, numPartitions, deadlineMs);
    boolean eternal = retentionTimeMs == PubSubConstants.ETERNAL_TOPIC_RETENTION_POLICY_MS;
    logger.info("Successfully created {}topic: {}", eternal ? "eternal " : "", topicName);
  }

  protected void waitUntilTopicCreated(PubSubTopic topicName, int partitionCount, long deadlineMs) {
    long startTime = System.currentTimeMillis();
    while (!containsTopicAndAllPartitionsAreOnline(topicName, partitionCount)) {
      if (System.currentTimeMillis() > deadlineMs) {
        throw new PubSubOpTimeoutException(
            "Timeout while creating topic: " + topicName + ".  Topic still did not pass all the checks after "
                + (deadlineMs - startTime) + "ms.");
      }
      Utils.sleep(200);
    }
  }

  /**
   * Update retention for the given topic.
   * If the topic doesn't exist, this operation will throw {@link PubSubTopicDoesNotExistException}
   * @param topicName
   * @param retentionInMS
   * @return true if the retention time config of the input topic gets updated; return false if nothing gets updated
   */
  public boolean updateTopicRetention(PubSubTopic topicName, long retentionInMS)
      throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topicName);
    return updateTopicRetention(topicName, retentionInMS, pubSubTopicConfiguration);
  }

  /**
   * Update retention for the given topic given a {@link Properties}.
   * @param topicName
   * @param expectedRetentionInMs
   * @param pubSubTopicConfiguration
   * @return true if the retention time gets updated; false if no update is needed.
   */
  public boolean updateTopicRetention(
      PubSubTopic topicName,
      long expectedRetentionInMs,
      PubSubTopicConfiguration pubSubTopicConfiguration) throws PubSubTopicDoesNotExistException {
    Optional<Long> retentionTimeMs = pubSubTopicConfiguration.retentionInMs();
    if (!retentionTimeMs.isPresent() || expectedRetentionInMs != retentionTimeMs.get()) {
      pubSubTopicConfiguration.setRetentionInMs(Optional.of(expectedRetentionInMs));
      pubSubAdminAdapter.setTopicConfig(topicName, pubSubTopicConfiguration);
      logger.info(
          "Updated topic: {} with retention.ms: {} in cluster [{}]",
          topicName,
          expectedRetentionInMs,
          this.pubSubClusterAddress);
      return true;
    }
    // Retention time has already been updated for this topic before
    return false;
  }

  public void updateTopicCompactionPolicy(PubSubTopic topic, boolean expectedLogCompacted) {
    updateTopicCompactionPolicy(topic, expectedLogCompacted, -1, Optional.empty());
  }

  /**
   * Update topic compaction policy.
   * @param topic
   * @param expectedLogCompacted
   * @param minLogCompactionLagMs the overrode min log compaction lag. If this is specified and a valid number (> 0), it will
   *                              override the default config
   * @throws PubSubTopicDoesNotExistException, if the topic doesn't exist
   */
  public void updateTopicCompactionPolicy(
      PubSubTopic topic,
      boolean expectedLogCompacted,
      long minLogCompactionLagMs,
      Optional<Long> maxLogCompactionLagMs) throws PubSubTopicDoesNotExistException {
    long expectedMinLogCompactionLagMs = 0l;
    Optional<Long> expectedMaxLogCompactionLagMs = maxLogCompactionLagMs;

    if (expectedLogCompacted) {
      if (minLogCompactionLagMs > 0) {
        expectedMinLogCompactionLagMs = minLogCompactionLagMs;
      } else {
        expectedMinLogCompactionLagMs = topicManagerContext.getTopicMinLogCompactionLagMs();
      }
      expectedMaxLogCompactionLagMs = maxLogCompactionLagMs;

      if (expectedMaxLogCompactionLagMs.isPresent()
          && expectedMaxLogCompactionLagMs.get() < expectedMinLogCompactionLagMs) {
        throw new VeniceException(
            "'expectedMaxLogCompactionLagMs': " + expectedMaxLogCompactionLagMs.get()
                + " shouldn't be smaller than 'expectedMinLogCompactionLagMs': " + expectedMinLogCompactionLagMs
                + " when updating compaction policy for topic: " + topic);
      }
    }

    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topic);
    boolean currentLogCompacted = pubSubTopicConfiguration.isLogCompacted();
    long currentMinLogCompactionLagMs = pubSubTopicConfiguration.minLogCompactionLagMs();
    Optional<Long> currentMaxLogCompactionLagMs = pubSubTopicConfiguration.getMaxLogCompactionLagMs();
    if (expectedLogCompacted != currentLogCompacted
        || expectedLogCompacted && (expectedMinLogCompactionLagMs != currentMinLogCompactionLagMs
            || expectedMaxLogCompactionLagMs.equals(currentMaxLogCompactionLagMs))) {
      pubSubTopicConfiguration.setLogCompacted(expectedLogCompacted);
      pubSubTopicConfiguration.setMinLogCompactionLagMs(expectedMinLogCompactionLagMs);
      pubSubTopicConfiguration.setMaxLogCompactionLagMs(expectedMaxLogCompactionLagMs);
      pubSubAdminAdapter.setTopicConfig(topic, pubSubTopicConfiguration);
      logger.info(
          "Kafka compaction policy for topic: {} has been updated from {} to {}, min compaction lag updated from"
              + " {} to {}, max compaction lag updated from {} to {}",
          topic,
          currentLogCompacted,
          expectedLogCompacted,
          currentMinLogCompactionLagMs,
          expectedMinLogCompactionLagMs,
          currentMaxLogCompactionLagMs.isPresent() ? currentMaxLogCompactionLagMs.get() : " not set",
          expectedMaxLogCompactionLagMs.isPresent() ? expectedMaxLogCompactionLagMs.get() : " not set");
    }
  }

  public boolean isTopicCompactionEnabled(PubSubTopic topicName) {
    PubSubTopicConfiguration topicProperties = getCachedTopicConfig(topicName);
    return topicProperties.isLogCompacted();
  }

  public long getTopicMinLogCompactionLagMs(PubSubTopic topicName) {
    PubSubTopicConfiguration topicProperties = getCachedTopicConfig(topicName);
    return topicProperties.minLogCompactionLagMs();
  }

  public Optional<Long> getTopicMaxLogCompactionLagMs(PubSubTopic topicName) {
    PubSubTopicConfiguration topicProperties = getCachedTopicConfig(topicName);
    return topicProperties.getMaxLogCompactionLagMs();
  }

  public boolean updateTopicMinInSyncReplica(PubSubTopic topicName, int minISR)
      throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topicName);
    Optional<Integer> currentMinISR = pubSubTopicConfiguration.minInSyncReplicas();
    // config doesn't exist config is different
    if (!currentMinISR.isPresent() || !currentMinISR.get().equals(minISR)) {
      pubSubTopicConfiguration.setMinInSyncReplicas(Optional.of(minISR));
      pubSubAdminAdapter.setTopicConfig(topicName, pubSubTopicConfiguration);
      logger.info("Updated topic: {} with min.insync.replicas: {}", topicName, minISR);
      return true;
    }
    // min.insync.replicas has already been updated for this topic before
    return false;
  }

  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    return pubSubAdminAdapter.getAllTopicRetentions();
  }

  /**
   * Return topic retention time in MS.
   */
  public long getTopicRetention(PubSubTopic topicName) throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topicName);
    return getTopicRetention(pubSubTopicConfiguration);
  }

  public long getTopicRetention(PubSubTopicConfiguration pubSubTopicConfiguration) {
    if (pubSubTopicConfiguration.retentionInMs().isPresent()) {
      return pubSubTopicConfiguration.retentionInMs().get();
    }
    return PubSubConstants.UNKNOWN_TOPIC_RETENTION;
  }

  /**
   * Check whether topic is absent or truncated
   * @param topicName
   * @param truncatedTopicMaxRetentionMs
   * @return true if the topic does not exist or if it exists but its retention time is below truncated threshold
   *         false if the topic exists and its retention time is above truncated threshold
   */
  public boolean isTopicTruncated(PubSubTopic topicName, long truncatedTopicMaxRetentionMs) {
    try {
      return isRetentionBelowTruncatedThreshold(getTopicRetention(topicName), truncatedTopicMaxRetentionMs);
    } catch (PubSubTopicDoesNotExistException e) {
      return true;
    }
  }

  public boolean isRetentionBelowTruncatedThreshold(long retention, long truncatedTopicMaxRetentionMs) {
    return retention != PubSubConstants.UNKNOWN_TOPIC_RETENTION && retention <= truncatedTopicMaxRetentionMs;
  }

  /**
   * This operation is a little heavy, since it will pull the configs for all the topics.
   */
  public PubSubTopicConfiguration getTopicConfig(PubSubTopic topicName) throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = pubSubAdminAdapter.getTopicConfig(topicName);
    topicConfigCache.put(topicName, pubSubTopicConfiguration);
    return pubSubTopicConfiguration;
  }

  public PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic topicName) {
    PubSubTopicConfiguration pubSubTopicConfiguration = pubSubAdminAdapter.getTopicConfigWithRetry(topicName);
    topicConfigCache.put(topicName, pubSubTopicConfiguration);
    return pubSubTopicConfiguration;
  }

  /**
   * Still heavy, but can be called repeatedly to amortize that cost.
   */
  public PubSubTopicConfiguration getCachedTopicConfig(PubSubTopic topicName) {
    // query the cache first, if it doesn't have it, query it from kafka and store it.
    PubSubTopicConfiguration pubSubTopicConfiguration = topicConfigCache.getIfPresent(topicName);
    if (pubSubTopicConfiguration == null) {
      pubSubTopicConfiguration = getTopicConfigWithRetry(topicName);
    }
    return pubSubTopicConfiguration;
  }

  public Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> topicNames) {
    Map<PubSubTopic, PubSubTopicConfiguration> topicConfigs = pubSubAdminAdapter.getSomeTopicConfigs(topicNames);
    for (Map.Entry<PubSubTopic, PubSubTopicConfiguration> topicConfig: topicConfigs.entrySet()) {
      topicConfigCache.put(topicConfig.getKey(), topicConfig.getValue());
    }
    return topicConfigs;
  }

  /**
   * Delete a topic and block until it is deleted or operation times out.
   * @param pubSubTopic topic to delete
   */
  public void ensureTopicIsDeletedAndBlock(PubSubTopic pubSubTopic) {
    if (!containsTopicAndAllPartitionsAreOnline(pubSubTopic)) {
      // Topic doesn't exist
      return;
    }

    logger.info("Deleting topic: {}", pubSubTopic);
    try {
      pubSubAdminAdapter.deleteTopic(pubSubTopic, Duration.ofMillis(topicManagerContext.getPubSubOperationTimeoutMs()));
      logger.info("Topic: {} has been deleted", pubSubTopic);
    } catch (PubSubOpTimeoutException e) {
      logger.warn(
          "Failed to delete topic: {} after {} ms",
          pubSubTopic,
          topicManagerContext.getPubSubOperationTimeoutMs());
    } catch (PubSubTopicDoesNotExistException e) {
      // No-op. Topic is deleted already, consider this as a successful deletion.
    } catch (PubSubClientRetriableException | PubSubClientException e) {
      logger.error("Failed to delete topic: {}", pubSubTopic, e);
      throw e;
    }

    // let's make sure the topic is deleted
    if (pubSubAdminAdapter.containsTopic(pubSubTopic)) {
      throw new PubSubTopicExistsException("Topic: " + pubSubTopic.getName() + " still exists after deletion");
    }
  }

  public void ensureTopicIsDeletedAndBlockWithRetry(PubSubTopic pubSubTopic) {
    int attempts = 0;
    while (attempts++ < PubSubConstants.MAX_TOPIC_DELETE_RETRIES) {
      try {
        logger.debug(
            "Deleting topic: {} with retry attempt {} / {}",
            pubSubTopic,
            attempts,
            PubSubConstants.MAX_TOPIC_DELETE_RETRIES);
        ensureTopicIsDeletedAndBlock(pubSubTopic);
        return;
      } catch (PubSubClientRetriableException e) {
        String errorMessage = e instanceof PubSubOpTimeoutException ? "timed out" : "errored out";
        logger.warn(
            "Topic deletion for topic: {} {}! Retry attempt {} / {}",
            pubSubTopic,
            errorMessage,
            attempts,
            PubSubConstants.MAX_TOPIC_DELETE_RETRIES);
        if (attempts == PubSubConstants.MAX_TOPIC_DELETE_RETRIES) {
          logger.error("Topic deletion for topic {} {}! Giving up!!", pubSubTopic, errorMessage, e);
          throw e;
        }
      }
    }
  }

  // TODO: Evaluate if we need synchronized here
  public synchronized Set<PubSubTopic> listTopics() {
    return pubSubAdminAdapter.listAllTopics();
  }

  /**
   * See Java doc of {@link PubSubAdminAdapter#containsTopicWithExpectationAndRetry} which provides exactly the same
   * semantics.
   */
  public boolean containsTopicWithExpectationAndRetry(
      PubSubTopic topic,
      int maxAttempts,
      final boolean expectedResult) {
    return pubSubAdminAdapter.containsTopicWithExpectationAndRetry(topic, maxAttempts, expectedResult);
  }

  public boolean containsTopicWithExpectationAndRetry(
      PubSubTopic topic,
      int maxAttempts,
      final boolean expectedResult,
      Duration initialBackoff,
      Duration maxBackoff,
      Duration maxDuration) {
    return pubSubAdminAdapter.containsTopicWithExpectationAndRetry(
        topic,
        maxAttempts,
        expectedResult,
        initialBackoff,
        maxBackoff,
        maxDuration);
  }

  /**
   * @see {@link #containsTopicAndAllPartitionsAreOnline(PubSubTopic, Integer)}
   */
  public boolean containsTopicAndAllPartitionsAreOnline(PubSubTopic topic) {
    return containsTopicAndAllPartitionsAreOnline(topic, null);
  }

  /**
   * This is an extensive check to mitigate an edge-case where a topic is not yet created in all the brokers.
   *
   * @return true if the topic exists and all its partitions have at least one in-sync replica
   *         false if the topic does not exist at all or if it exists but isn't completely available
   */
  public boolean containsTopicAndAllPartitionsAreOnline(PubSubTopic topic, Integer expectedPartitionCount) {
    if (!containsTopic(topic)) {
      return false;
    }
    List<PubSubTopicPartitionInfo> partitionInfoList = topicMetadataFetcher.getTopicPartitionInfo(topic);
    if (partitionInfoList == null) {
      logger.warn("getConsumer().partitionsFor() returned null for topic: {}", topic);
      return false;
    }

    if (expectedPartitionCount != null && partitionInfoList.size() != expectedPartitionCount) {
      // Unexpected. Should perhaps even throw...
      logger.error(
          "getConsumer().partitionsFor() returned the wrong number of partitions for topic: {}, "
              + "expectedPartitionCount: {}, actual size: {}, partitionInfoList: {}",
          topic,
          expectedPartitionCount,
          partitionInfoList.size(),
          Arrays.toString(partitionInfoList.toArray()));
      return false;
    }

    boolean allPartitionsHaveAnInSyncReplica =
        partitionInfoList.stream().allMatch(PubSubTopicPartitionInfo::hasInSyncReplicas);
    if (allPartitionsHaveAnInSyncReplica) {
      logger.trace("The following topic has the at least one in-sync replica for each partition: {}", topic);
    } else {
      logger.info(
          "getConsumer().partitionsFor() returned some partitionInfo with no in-sync replica for topic: {}, partitionInfoList: {}",
          topic,
          Arrays.toString(partitionInfoList.toArray()));
    }
    return allPartitionsHaveAnInSyncReplica;
  }

  // For testing only
  public void setTopicConfigCache(Cache<PubSubTopic, PubSubTopicConfiguration> topicConfigCache) {
    this.topicConfigCache = topicConfigCache;
  }

  /**
   * Get information about all partitions for a given topic.
   * @param pubSubTopic the topic to get partition info for
   * @return a list of {@link PubSubTopicPartitionInfo} for the given topic
   */
  public List<PubSubTopicPartitionInfo> getTopicPartitionInfo(PubSubTopic pubSubTopic) {
    return topicMetadataFetcher.getTopicPartitionInfo(pubSubTopic);
  }

  /**
   * Get the latest offsets for all partitions of a given topic.
   * @param pubSubTopic the topic to get latest offsets for
   * @return a Map of partition to the latest offset, or an empty map if there's any problem getting the offsets
   */
  public Int2LongMap getTopicLatestOffsets(PubSubTopic pubSubTopic) {
    return topicMetadataFetcher.getTopicLatestOffsets(pubSubTopic);
  }

  public int getPartitionCount(PubSubTopic pubSubTopic) {
    return getTopicPartitionInfo(pubSubTopic).size();
  }

  public boolean containsTopic(PubSubTopic pubSubTopic) {
    return topicMetadataFetcher.containsTopic(pubSubTopic);
  }

  public boolean containsTopicCached(PubSubTopic pubSubTopic) {
    return topicMetadataFetcher.containsTopicCached(pubSubTopic);
  }

  public long getLatestOffsetWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return topicMetadataFetcher.getLatestOffsetWithRetries(pubSubTopicPartition, retries);
  }

  public long getLatestOffsetCached(PubSubTopic pubSubTopic, int partitionId) {
    return topicMetadataFetcher.getLatestOffsetCached(new PubSubTopicPartitionImpl(pubSubTopic, partitionId));
  }

  public long getProducerTimestampOfLastDataMessageWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return topicMetadataFetcher.getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, retries);
  }

  public long getProducerTimestampOfLastDataMessageCached(PubSubTopicPartition pubSubTopicPartition) {
    return topicMetadataFetcher.getProducerTimestampOfLastDataMessageCached(pubSubTopicPartition);
  }

  /**
   * Get offsets for only one partition with a specific timestamp.
   */
  public long getPartitionOffsetByTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    return topicMetadataFetcher.getOffsetForTimeWithRetries(pubSubTopicPartition, timestamp, 5);
  }

  /**
   * Invalidate the cache for the given topic and all its partitions.
   * @param pubSubTopic the topic to invalidate
   */
  public CompletableFuture<Void> invalidateCache(PubSubTopic pubSubTopic) {
    return topicMetadataFetcher.invalidateKeyAsync(pubSubTopic);
  }

  public void prefetchAndCacheLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    topicMetadataFetcher.populateCacheWithLatestOffset(pubSubTopicPartition);
  }

  public String getPubSubClusterAddress() {
    return this.pubSubClusterAddress;
  }

  @Override
  public synchronized void close() {
    Utils.closeQuietlyWithErrorLogged(topicMetadataFetcher);
    Utils.closeQuietlyWithErrorLogged(pubSubAdminAdapter);
  }
}
