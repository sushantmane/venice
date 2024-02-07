package com.linkedin.venice.pubsub.manager;

import static org.testng.Assert.assertFalse;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TopicManagerITest {
  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private TopicManager topicManager;
  private TopicManagerContext.Builder topicManagerContextBuilder;
  private PubSubTopicRepository pubSubTopicRepository;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    pubSubTopicRepository = new PubSubTopicRepository();
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();

    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.setProperty(PubSubConstants.PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE, "true");
    properties.putAll(pubSubBrokerWrapper.getAdditionalConfig());
    properties.putAll(pubSubBrokerWrapper.getMergeableConfigs());
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    topicManagerContextBuilder = new TopicManagerContext.Builder().setTopicOffsetCheckIntervalMs(1000)
        .setPubSubOperationTimeoutMs(1000)
        .setPubSubAdminAdapterFactory(pubSubBrokerWrapper.getPubSubClientsFactory().getAdminAdapterFactory())
        .setPubSubConsumerAdapterFactory(pubSubBrokerWrapper.getPubSubClientsFactory().getConsumerAdapterFactory())
        .setPubSubTopicRepository(new PubSubTopicRepository())
        .setPubSubPropertiesSupplier(k -> veniceProperties)
        .setMetricsRepository(new MetricsRepository())
        .setTopicMetadataFetcherConsumerPoolSize(2)
        .setTopicMetadataFetcherThreadPoolSize(3);
  }

  @BeforeMethod(alwaysRun = true)
  public void setUpMethod() {
    topicManager = new TopicManager(pubSubBrokerWrapper.getAddress(), topicManagerContextBuilder.build());
  }

  @Test
  public void testCreateTopic() {
    String topicName = Utils.getUniqueString("test-topic");
    int numPartitions = 3;
    int replicationFactor = 1;
    boolean isEternalTopic = true;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    assertFalse(topicManager.containsTopic(pubSubTopic));
    topicManager.createTopic(pubSubTopic, numPartitions, replicationFactor, isEternalTopic);
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> topicManager.containsTopic(pubSubTopic));
  }

  @Test
  public void testGetAllTopicRetentions() {

  }

}
