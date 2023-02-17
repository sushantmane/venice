package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;


public class ApacheKafkaProduceResultTest {
  @Test(expectedExceptions = NullPointerException.class)
  public void testApacheKafkaProduceResultShouldThrowNPEWhenRecordMetadataIsNull() {
    new ApacheKafkaProduceResult(null);
  }

  @Test
  public void testApacheKafkaProduceResult() {
    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topicX", 42), 1, 2, 3, 4L, -5, -6);
    PubSubProduceResult produceResult = new ApacheKafkaProduceResult(recordMetadata);
    assertNotNull(produceResult);
    assertEquals(produceResult.topic(), recordMetadata.topic());
    assertEquals(produceResult.partition(), recordMetadata.partition());
    assertEquals(produceResult.offset(), recordMetadata.offset());
    assertEquals(produceResult.serializedKeySize(), recordMetadata.serializedKeySize());
    assertEquals(produceResult.serializedValueSize(), recordMetadata.serializedValueSize());
  }
}
