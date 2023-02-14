package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.adapter.SimplePubsubProducerCallbackImpl;
import com.linkedin.venice.pubsub.api.ProduceResult;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.testng.annotations.Test;


public class ApacheKafkaProducerCallbackTest {
  @Test
  public void testOnCompletionShouldInvokeInternalCallback() {
    SimplePubsubProducerCallbackImpl internalCallback = new SimplePubsubProducerCallbackImpl();
    ApacheKafkaProducerCallback kafkaProducerCallback = new ApacheKafkaProducerCallback(internalCallback);
    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topicX", 42), 0, 1, 1676397545, 1L, 11, 12);

    kafkaProducerCallback.onCompletion(recordMetadata, null);
    assertTrue(internalCallback.isInvoked());
    assertNull(internalCallback.exception());
    assertNotNull(internalCallback.producerResult());
    ProduceResult produceResult = internalCallback.producerResult();
    assertEquals(produceResult.topic(), recordMetadata.topic());
    assertEquals(produceResult.partition(), recordMetadata.partition());
    assertEquals(produceResult.offset(), recordMetadata.offset());
  }

  @Test
  public void testOnCompletionShouldInvokeInternalCallbackWithException() {
    SimplePubsubProducerCallbackImpl internalCallback = new SimplePubsubProducerCallbackImpl();
    ApacheKafkaProducerCallback kafkaProducerCallback = new ApacheKafkaProducerCallback(internalCallback);
    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topicX", 42), -1, -1, -1, -1L, -1, -1);

    UnknownTopicOrPartitionException exception = new UnknownTopicOrPartitionException("Unknown topic: topicX");

    kafkaProducerCallback.onCompletion(recordMetadata, exception);
    assertTrue(internalCallback.isInvoked());
    assertNotNull(internalCallback.exception());
    assertEquals(internalCallback.exception(), exception);

    assertNotNull(internalCallback.producerResult());
    ProduceResult produceResult = internalCallback.producerResult();
    assertEquals(produceResult.topic(), recordMetadata.topic());
    assertEquals(produceResult.partition(), recordMetadata.partition());
    assertEquals(produceResult.offset(), recordMetadata.offset());
  }
}
