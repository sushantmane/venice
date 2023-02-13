package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaProducerAdapterTest {
  private KafkaProducer<KafkaKey, KafkaMessageEnvelope> mockKafkaProducer;
  private ApacheKafkaProducerConfig mockProducerConfig;
  private final String topicName = "test-topic";
  private final KafkaKey testKafkaKey = new KafkaKey(MessageType.DELETE, "key".getBytes());
  private final KafkaMessageEnvelope testKafkaValue = new KafkaMessageEnvelope();

  @BeforeMethod
  public void setupMocks() {
    mockKafkaProducer = mock(KafkaProducer.class);
    mockProducerConfig = mock(ApacheKafkaProducerConfig.class);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "The internal KafkaProducer has been closed")
  public void testEnsureProducerIsNotClosedThrowsExceptionWhenProducerIsClosed() {
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(mockProducerConfig, mockKafkaProducer);
    Mockito.doNothing().when(mockKafkaProducer).close(any());
    producerAdapter.close(10, false);
    producerAdapter.sendMessage(topicName, 0, testKafkaKey, testKafkaValue, null, null);
  }

  @Test
  public void testGetNumberOfPartitions() {
    List<PartitionInfo> list = new ArrayList<>();
    when(mockKafkaProducer.partitionsFor(topicName)).thenReturn(list);

    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(mockProducerConfig, mockKafkaProducer);
    Assert.assertEquals(producerAdapter.getNumberOfPartitions(topicName), 0);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Got an error while trying to produce message into Kafka.*")
  public void testSendMessageThrowsAnExceptionOnTimeout() {
    doThrow(TimeoutException.class).when(mockKafkaProducer).send(any(), any());
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(mockProducerConfig, mockKafkaProducer);
    producerAdapter.sendMessage(topicName, 42, testKafkaKey, testKafkaValue, null, null);
  }
}
