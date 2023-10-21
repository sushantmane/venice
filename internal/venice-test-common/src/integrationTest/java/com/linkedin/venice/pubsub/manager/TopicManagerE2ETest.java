package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.PubSubHelper;
import com.linkedin.venice.utils.PubSubHelper.MutablePubSubMessage;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TopicManagerE2ETest {
  // timeout for pub-sub operations
  private static final Duration PUBSUB_OP_TIMEOUT = Duration.ofSeconds(15);
  // add a variance of 5 seconds to the timeout to account for fluctuations in the test environment
  private static final long PUBSUB_OP_TIMEOUT_WITH_VARIANCE = PUBSUB_OP_TIMEOUT.toMillis() + 5000;
  // timeout for pub-sub consumer APIs which do not have a timeout parameter
  private static final int PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS = 10_000;
  // add a variance of 5 seconds to the timeout to account for fluctuations in the test environment
  private static final long PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_WITH_VARIANCE =
      PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS + 5000;
  private static final int REPLICATION_FACTOR = 1;
  private static final boolean IS_LOG_COMPACTED = false;
  private static final int MIN_IN_SYNC_REPLICAS = 1;
  private static final long RETENTION_IN_MS = Duration.ofDays(3).toMillis();
  private static final long MIN_LOG_COMPACTION_LAG_MS = Duration.ofDays(1).toMillis();
  private static final long MAX_LOG_COMPACTION_LAG_MS = Duration.ofDays(2).toMillis();
  private static final PubSubTopicConfiguration TOPIC_CONFIGURATION = new PubSubTopicConfiguration(
      Optional.of(RETENTION_IN_MS),
      IS_LOG_COMPACTED,
      Optional.of(MIN_IN_SYNC_REPLICAS),
      MIN_LOG_COMPACTION_LAG_MS,
      Optional.of(MAX_LOG_COMPACTION_LAG_MS));

  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private Lazy<PubSubAdminAdapter> pubSubAdminAdapterLazy;
  private Lazy<PubSubProducerAdapter> pubSubProducerAdapterLazy;
  private Lazy<PubSubConsumerAdapter> pubSubConsumerAdapterLazy;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private PubSubTopicRepository pubSubTopicRepository;
  private PubSubClientsFactory pubSubClientsFactory;
  private TopicManagerRepository topicManagerRepository;
  private TopicManager topicManager;
  private TopicManagerContext.Builder topicManagerContextBuilder;
  private MetricsRepository metricsRepository;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    pubSubMessageDeserializer = PubSubMessageDeserializer.getInstance();
    pubSubTopicRepository = new PubSubTopicRepository();
    pubSubClientsFactory = pubSubBrokerWrapper.getPubSubClientsFactory();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper);
  }

  @BeforeMethod(alwaysRun = true)
  public void setUpMethod() {
    String clientId = Utils.getUniqueString("TopicManageE2EITest");
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.setProperty(
        PubSubConstants.PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS,
        String.valueOf(PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS));
    properties.setProperty(PubSubConstants.PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE, "true");
    properties.putAll(pubSubBrokerWrapper.getAdditionalConfig());
    properties.putAll(pubSubBrokerWrapper.getMergeableConfigs());
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    pubSubProducerAdapterLazy =
        Lazy.of(() -> pubSubClientsFactory.getProducerAdapterFactory().create(veniceProperties, clientId, null));
    pubSubAdminAdapterLazy =
        Lazy.of(() -> pubSubClientsFactory.getAdminAdapterFactory().create(veniceProperties, pubSubTopicRepository));
    pubSubConsumerAdapterLazy = Lazy.of(
        () -> pubSubClientsFactory.getConsumerAdapterFactory()
            .create(veniceProperties, false, pubSubMessageDeserializer, clientId));

    metricsRepository = new MetricsRepository();
    topicManagerContextBuilder = new TopicManagerContext.Builder().setPubSubTopicRepository(pubSubTopicRepository)
        .setMetricsRepository(metricsRepository)
        .setTopicMetadataFetcherConsumerPoolSize(2)
        .setTopicMetadataFetcherThreadPoolSize(6)
        .setTopicOffsetCheckIntervalMs(60_0000)
        .setPubSubPropertiesSupplier(k -> veniceProperties)
        .setPubSubAdminAdapterFactory(pubSubClientsFactory.getAdminAdapterFactory())
        .setPubSubConsumerAdapterFactory(pubSubClientsFactory.getConsumerAdapterFactory());

    topicManagerRepository =
        new TopicManagerRepository(topicManagerContextBuilder.build(), pubSubBrokerWrapper.getAddress());
    topicManager = topicManagerRepository.getLocalTopicManager();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownMethod() {
    if (pubSubProducerAdapterLazy.isPresent()) {
      pubSubProducerAdapterLazy.get().close(0, false);
    }
    if (pubSubAdminAdapterLazy.isPresent()) {
      Utils.closeQuietlyWithErrorLogged(pubSubAdminAdapterLazy.get());
    }
    if (pubSubConsumerAdapterLazy.isPresent()) {
      Utils.closeQuietlyWithErrorLogged(pubSubConsumerAdapterLazy.get());
    }

    if (topicManagerRepository != null) {
      Utils.closeQuietlyWithErrorLogged(topicManagerRepository);
    }
  }

  @Test(timeOut = 5 * Time.MS_PER_MINUTE, invocationCount = 1000)
  public void testAsyncApis() throws ExecutionException, InterruptedException, TimeoutException {
    int numPartitions = 3;
    int replicationFactor = 1;
    boolean isEternalTopic = true;
    PubSubTopic testTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("testTopic"));
    PubSubTopic nonExistentTopic = pubSubTopicRepository.getTopic(Utils.getUniqueString("nonExistentTopic"));
    assertFalse(topicManager.containsTopic(testTopic));
    assertFalse(topicManager.containsTopic(nonExistentTopic));
    topicManager.createTopic(testTopic, numPartitions, replicationFactor, isEternalTopic);
    waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> topicManager.containsTopic(testTopic));

    int numMessages = 250;
    PubSubProducerAdapter pubSubProducerAdapter = pubSubProducerAdapterLazy.get();
    CompletableFuture<PubSubProduceResult> lastMessageFuture = null;
    // list of messages
    Map<Integer, MutablePubSubMessage> messages = new HashMap<>(numMessages);
    for (int i = 0; i < numMessages; i++) {
      MutablePubSubMessage message = PubSubHelper.getDummyPubSubMessage(false);
      message.getValue().getProducerMetadata().setMessageTimestamp(i);
      messages.put(i, message);
      lastMessageFuture =
          pubSubProducerAdapter.sendMessage(testTopic.getName(), 0, message.getKey(), message.getValue(), null, null);
      lastMessageFuture.whenComplete((result, throwable) -> {
        if (throwable == null) {
          message.setOffset(result.getOffset());
        }
      });
    }
    assertNotNull(lastMessageFuture, "Last message future should not be null");
    lastMessageFuture.get(1, TimeUnit.MINUTES);

    final AtomicInteger successfulRequests = new AtomicInteger(0);
    List<Runnable> tasks = new ArrayList<>();

    Runnable getPartitionCountTask = () -> {
      try {
        int actualNumPartitions = topicManager.getPartitionCount(testTopic);
        assertEquals(actualNumPartitions, numPartitions);
        successfulRequests.incrementAndGet();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    tasks.add(getPartitionCountTask);

    // get partition count for non-existent topic
    Runnable getPartitionCountForNonExistentTopicTask = () -> {
      try {
        assertNull(topicManager.getPartitionCount(nonExistentTopic));
        successfulRequests.incrementAndGet();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    tasks.add(getPartitionCountForNonExistentTopicTask);

    // contains topic
    Runnable containsTopicTask = () -> {
      try {
        assertTrue(topicManager.containsTopic(testTopic));
        successfulRequests.incrementAndGet();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    tasks.add(containsTopicTask);

    // contains topic for non-existent topic
    Runnable containsNonExistentTopicTask = () -> {
      try {
        assertFalse(topicManager.containsTopic(nonExistentTopic));
        successfulRequests.incrementAndGet();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    tasks.add(containsNonExistentTopicTask);

    Runnable getLatestOffsetWithRetriesTask = () -> {
      try {
        long latestOffset = topicManager.getLatestOffsetWithRetries(new PubSubTopicPartitionImpl(testTopic, 0), 1);
        assertEquals(latestOffset, numMessages);
        successfulRequests.incrementAndGet();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    tasks.add(getLatestOffsetWithRetriesTask);

    ExecutorService executorService = Executors.newFixedThreadPool(16);

    List<Future> vwFutures = new ArrayList<>();

    int totalTasks = 4096;
    for (int i = 0; i < totalTasks; i++) {
      Future future = executorService.submit(tasks.get(i % tasks.size()));
      vwFutures.add(future);
    }

    int failedRequests = 0;
    for (Future future: vwFutures) {
      try {
        future.get(1, TimeUnit.MINUTES);
      } catch (Exception e) {
        failedRequests++;
      }
    }
    System.out.println("successfulRequests: " + successfulRequests.get());
    // total should be equal to the number of tasks
    assertEquals(successfulRequests.get() + failedRequests, totalTasks);
  }

}
