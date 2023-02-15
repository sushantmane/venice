package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CLIENT_ID;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.SharedKafkaProducerConfig.SHARED_KAFKA_PRODUCER_CONFIG_PREFIX;
import static com.linkedin.venice.writer.VeniceWriter.CLOSE_TIMEOUT_MS;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_CLOSE_TIMEOUT_MS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.ProducerAdapter;
import com.linkedin.venice.pubsub.api.ProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service maintains a pool of kafka producer. Ingestion task can acquire or release a producer on demand basis.
 * It does lazy initialization of producers. Also producers are assigned based on least loaded manner.
 */
public class SharedKafkaProducerAdapterFactory implements ProducerAdapterFactory<SharedKafkaProducerAdapter> {
  private static final Logger LOGGER = LogManager.getLogger(SharedKafkaProducerAdapterFactory.class);
  public static final String NAME = "ApacheKafkaSharedProducer";
  private final int numOfProducersPerKafkaCluster;
  private final Properties producerProperties;
  private final int kafkaProducerCloseTimeout;

  private final SharedKafkaProducerAdapter[] producers;
  private final Map<String, SharedKafkaProducerAdapter> producerTaskToProducerMap = new VeniceConcurrentHashMap<>();
  private final ApacheKafkaProducerAdapterFactory internalProducerAdapterFactory;
  private volatile boolean isRunning = true;

  // stats
  private final MetricsRepository metricsRepository;
  private final Set<String> producerMetricsToBeReported;
  private final SharedProducerServiceStats sharedProducerServiceStats;
  final AtomicLong activeSharedProducerTasksCount = new AtomicLong(0);
  final AtomicLong activeSharedProducerCount = new AtomicLong(0);

  /**
   *
   * @param properties -- List of properties to construct a kafka producer
   * @param sharedProducerPoolCount  -- producer pool sizes
   * @param internalProducerAdapterFactory -- factory to create a KafkaProducerAdapter object
   * @param metricsRepository -- metric repository
   * @param producerMetricsToBeReported -- a comma seperated list of KafkaProducer metrics that will exported as ingraph metrics
   *
   * Note: This producer will not work when target topic is in different fabric than the localKafkaBootstrapServers.
   */
  public SharedKafkaProducerAdapterFactory(
      Properties properties,
      int sharedProducerPoolCount,
      ApacheKafkaProducerAdapterFactory internalProducerAdapterFactory,
      MetricsRepository metricsRepository,
      Set<String> producerMetricsToBeReported) {
    this.internalProducerAdapterFactory = internalProducerAdapterFactory;
    this.producerProperties = new Properties();
    this.producerProperties.putAll(properties);
    VeniceProperties veniceWriterProperties = new VeniceProperties(this.producerProperties);
    this.kafkaProducerCloseTimeout = veniceWriterProperties.getInt(CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS);

    // replace all properties starting with SHARED_KAFKA_PRODUCER_CONFIG_PREFIX with KAFKA_CONFIG_PREFIX.
    Properties sharedProducerProperties =
        veniceWriterProperties.clipAndFilterNamespace(SHARED_KAFKA_PRODUCER_CONFIG_PREFIX).toProperties();
    for (Map.Entry<Object, Object> entry: sharedProducerProperties.entrySet()) {
      this.producerProperties.put(KAFKA_CONFIG_PREFIX + entry.getKey(), entry.getValue());
    }

    this.numOfProducersPerKafkaCluster = sharedProducerPoolCount;
    this.producers = new SharedKafkaProducerAdapter[numOfProducersPerKafkaCluster];
    this.metricsRepository = metricsRepository;
    this.producerMetricsToBeReported = producerMetricsToBeReported;
    this.sharedProducerServiceStats =
        metricsRepository != null ? new SharedProducerServiceStats(metricsRepository, this) : null;
    LOGGER.info("SharedKafkaProducerAdapter: is initialized");
  }

  @Override
  public synchronized void close() {
    isRunning = false;
    LOGGER.info("SharedKafkaProducerAdapter: is being closed");
    // This map should be empty when this is called.
    if (!producerTaskToProducerMap.isEmpty()) {
      LOGGER.error(
          "SharedKafkaProducerAdapter: following producerTasks are still using the shared producers. [{}]",
          producerTaskToProducerMap.keySet().stream().collect(Collectors.joining(",")));
    }

    Set<SharedKafkaProducerAdapter> producerInstanceSet = new HashSet<>(Arrays.asList(producers));
    producerInstanceSet.parallelStream().filter(Objects::nonNull).forEach(sharedKafkaProducer -> {
      try {
        // Force close all the producer even if there are active producerTask assigned to it.
        LOGGER.info(
            "SharedKafkaProducerAdapter: Closing producer: {}, Currently assigned task: {}",
            sharedKafkaProducer,
            sharedKafkaProducer.getProducerTaskCount());
        sharedKafkaProducer.close(kafkaProducerCloseTimeout, false);
        producers[sharedKafkaProducer.getId()] = null;
        decrActiveSharedProducerCount();
      } catch (Exception e) {
        LOGGER.warn("SharedKafkaProducerAdapter: Error in closing kafka producer", e);
      }
    });
  }

  public boolean isRunning() {
    return isRunning;
  }

  // TODO: use topic + destination borker address combination for producerTaskName
  public synchronized SharedKafkaProducerAdapter acquireKafkaProducer(String producerTaskName) {
    if (!isRunning) {
      throw new VeniceException(
          "SharedKafkaProducerAdapter: is already closed, can't assign new producer for task:" + producerTaskName);
    }

    SharedKafkaProducerAdapter sharedKafkaProducer = null;

    if (producerTaskToProducerMap.containsKey(producerTaskName)) {
      sharedKafkaProducer = producerTaskToProducerMap.get(producerTaskName);
      LOGGER.info(
          "SharedKafkaProducerAdapter: {} already has a producer id: {}",
          producerTaskName,
          sharedKafkaProducer.getId());
      return sharedKafkaProducer;
    }

    // Do lazy creation of producers
    for (int i = 0; i < producers.length; i++) {
      if (producers[i] == null) {
        LOGGER.info("SharedKafkaProducerAdapter: Creating Producer id: {}", i);
        producerProperties.put(KAFKA_CLIENT_ID, "shared-producer-" + i);
        ProducerAdapter producerAdapter = internalProducerAdapterFactory
            .create(new VeniceProperties(producerProperties), "shared-producer-" + i, null);
        sharedKafkaProducer =
            new SharedKafkaProducerAdapter(this, i, producerAdapter, metricsRepository, producerMetricsToBeReported);
        producers[i] = sharedKafkaProducer;
        LOGGER.info("SharedKafkaProducerAdapter: Created Shared Producer instance: {}", sharedKafkaProducer);
        incrActiveSharedProducerCount();
        break;
      }
    }

    // Find the least used producer instance
    if (sharedKafkaProducer == null) {
      int minProducerTaskCount = Integer.MAX_VALUE;
      for (int i = 0; i < producers.length; i++) {
        if (producers[i].getProducerTaskCount() < minProducerTaskCount) {
          minProducerTaskCount = producers[i].getProducerTaskCount();
          sharedKafkaProducer = producers[i];
        }
      }
    }

    sharedKafkaProducer.addProducerTask(producerTaskName);
    producerTaskToProducerMap.put(producerTaskName, sharedKafkaProducer);
    LOGGER.info(
        "SharedKafkaProducerAdapter: {} acquired the producer id: {}",
        producerTaskName,
        sharedKafkaProducer.getId());
    logProducerInstanceAssignments();
    incrActiveSharedProducerTasksCount();
    return sharedKafkaProducer;
  }

  public synchronized void releaseKafkaProducer(String producerTaskName) {
    if (!isRunning) {
      throw new VeniceException(
          "SharedKafkaProducerAdapter: is already closed, can't release the producer for task:" + producerTaskName);
    }

    if (!producerTaskToProducerMap.containsKey(producerTaskName)) {
      LOGGER.error("SharedKafkaProducerAdapter: {} does not have a producer", producerTaskName);
      return;
    }
    SharedKafkaProducerAdapter sharedKafkaProducer = producerTaskToProducerMap.get(producerTaskName);
    sharedKafkaProducer.removeProducerTask(producerTaskName);
    producerTaskToProducerMap.remove(producerTaskName, sharedKafkaProducer);
    LOGGER.info(
        "SharedKafkaProducerAdapter: {} released the producer id: {}",
        producerTaskName,
        sharedKafkaProducer.getId());
    logProducerInstanceAssignments();
    decrActiveSharedProducerTasksCount();
  }

  /**
   * This will print a log line consisting of how each producer instances are shared among producerTasks. It prints
   * the following tuple for each ProducerInstance
   * {producerId : count of producerTask using this producer}
   *
   * An example is following.
   * Current Assignments: [{Id: 0, Task Count: 1},{Id: 1, Task Count: 1},{Id: 2, Task Count: 1},{Id: 3, Task Count: 1},{Id: 4, Task Count: 1},]
   *
   * This is purely for debugging purpose to check the producers are evenly loaded.
   */
  private void logProducerInstanceAssignments() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < producers.length; i++) {
      if (producers[i] != null) {
        sb.append(producers[i].toString()).append(",");
      }
    }
    sb.append("]");
    LOGGER.info("SharedKafkaProducerAdapter: Current Assignments: {}", sb);
  }

  /**
   * N.B. This shared producer does not support brokerAddressToOverride. Do not use this in controllers.
   *
   * @param veniceProperties
   * @return
   */
  @Override
  public SharedKafkaProducerAdapter create(
      VeniceProperties veniceProperties,
      String topicName,
      String brokerAddressToOverride) {
    return acquireKafkaProducer(topicName);
  }

  public long getActiveSharedProducerTasksCount() {
    return activeSharedProducerTasksCount.get();
  }

  public long getActiveSharedProducerCount() {
    return activeSharedProducerCount.get();
  }

  private void incrActiveSharedProducerTasksCount() {
    activeSharedProducerTasksCount.incrementAndGet();
  }

  private void decrActiveSharedProducerTasksCount() {
    activeSharedProducerTasksCount.decrementAndGet();
  }

  private void incrActiveSharedProducerCount() {
    activeSharedProducerCount.incrementAndGet();
  }

  private void decrActiveSharedProducerCount() {
    activeSharedProducerCount.decrementAndGet();
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }
}
