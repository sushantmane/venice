package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.SimplePubsubProducerCallbackImpl;
import com.linkedin.venice.pubsub.api.ProduceResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaProducerAdapterTest {
  private KafkaProducer<KafkaKey, KafkaMessageEnvelope> mockKafkaProducer;
  private ApacheKafkaProducerConfig mockProducerConfig;
  private static final String TOPIC_NAME = "test-topic";
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
    producerAdapter.sendMessage(TOPIC_NAME, 0, testKafkaKey, testKafkaValue, null, null);
  }

  @Test
  public void testGetNumberOfPartitions() {
    List<PartitionInfo> list = new ArrayList<>();
    when(mockKafkaProducer.partitionsFor(TOPIC_NAME)).thenReturn(list);

    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(mockProducerConfig, mockKafkaProducer);
    Assert.assertEquals(producerAdapter.getNumberOfPartitions(TOPIC_NAME), 0);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Got an error while trying to produce message into Kafka.*")
  public void testSendMessageThrowsAnExceptionOnTimeout() {
    doThrow(TimeoutException.class).when(mockKafkaProducer).send(any(), any());
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(mockProducerConfig, mockKafkaProducer);
    producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, null);
  }

  @Test(expectedExceptions = InterruptedException.class, expectedExceptionsMessageRegExp = "Failed to complete request")
  public void testSendMessageThrowsExceptionWhenBlockingCallThrowsException()
      throws ExecutionException, InterruptedException {
    Future<RecordMetadata> recordMetadataFutureMock = mock(Future.class);
    when(mockKafkaProducer.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(recordMetadataFutureMock);
    when(recordMetadataFutureMock.get()).thenThrow(new InterruptedException("Failed to complete request"));

    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(mockProducerConfig, mockKafkaProducer);
    producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, null).get(); // blocking produce
                                                                                                 // call
  }

  @Test
  public void testSendMessageInteractionWithInternalProducer() {
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(mockProducerConfig, mockKafkaProducer);
    Future<RecordMetadata> recordMetadataFutureMock = mock(Future.class);

    // interaction (1) when callback is null
    when(mockKafkaProducer.send(any(ProducerRecord.class), isNull())).thenReturn(recordMetadataFutureMock);
    Future<ProduceResult> produceResultFuture =
        producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, null);
    Assert.assertNotNull(produceResultFuture);
    verify(mockKafkaProducer, times(1)).send(any(ProducerRecord.class), isNull());
    verify(mockKafkaProducer, never()).send(any(ProducerRecord.class), any(Callback.class));

    // interaction (1) when callback is non-null
    SimplePubsubProducerCallbackImpl producerCallback = new SimplePubsubProducerCallbackImpl();
    when(mockKafkaProducer.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(recordMetadataFutureMock);
    produceResultFuture =
        producerAdapter.sendMessage(TOPIC_NAME, 42, testKafkaKey, testKafkaValue, null, producerCallback);
    Assert.assertNotNull(produceResultFuture);
    verify(mockKafkaProducer, times(1)).send(any(ProducerRecord.class), isNull()); // from interaction (1)
    verify(mockKafkaProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  public void testCloseInvokesProducerFlushAndClose() {
    doNothing().when(mockKafkaProducer).flush(anyLong(), any(TimeUnit.class));
    ApacheKafkaProducerAdapter producerAdapter = new ApacheKafkaProducerAdapter(mockProducerConfig, mockKafkaProducer);
    producerAdapter.close(10);
    verify(mockKafkaProducer, times(1)).flush(anyLong(), any(TimeUnit.class));
    verify(mockKafkaProducer, times(1)).close(any(Duration.class));
  }
}
