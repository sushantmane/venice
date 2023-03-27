package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PartitionOffsetFetcherTest {
  private PubSubBrokerWrapper pubSubBrokerWrapper;

  @BeforeClass
  public void setUp() {
    this.pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
  }

  @AfterClass
  public void close() {
    this.pubSubBrokerWrapper.close();
  }

  @Test
  public void testGetPartitionLatestOffsetAndRetry() {
    KafkaClientFactory kafkaClientFactory = IntegrationTestPushUtils.getVeniceConsumerFactory(pubSubBrokerWrapper);
    try (PartitionOffsetFetcher fetcher = PartitionOffsetFetcherFactory.createDefaultPartitionOffsetFetcher(
        kafkaClientFactory,
        Lazy.of(() -> kafkaClientFactory.getKafkaAdminClient(Optional.empty())),
        1 * Time.MS_PER_SECOND,
        Optional.empty())) {
      String topic = Utils.getUniqueString("topic");
      Assert
          .assertThrows(TopicDoesNotExistException.class, () -> fetcher.getPartitionLatestOffsetAndRetry(topic, 0, 1));
    }
  }
}
