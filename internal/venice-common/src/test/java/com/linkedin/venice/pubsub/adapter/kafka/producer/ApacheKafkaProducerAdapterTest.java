package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaProducerAdapterTest {
  private KafkaProducer<KafkaKey, KafkaMessageEnvelope> mockKafkaProducer;
  private ApacheKafkaProducerConfig mockProducerConfig;

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
    producerAdapter.getNumberOfPartitions("x");
  }
}
