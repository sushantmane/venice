package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;


public class ApacheKafkaProduceResultFutureTest {
  @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "RecordMetadata future cannot be null")
  public void testApacheKafkaProduceResultFutureShouldThrowNpeIfRecordMetadataFutureIsNull() {
    new ApacheKafkaProduceResultFuture(null);
  }

  @Test
  public void testApacheKafkaProduceResultFutureCompletedWhenInnerFutureIsCompleted()
      throws ExecutionException, InterruptedException {
    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topicX", 42), -1, -1, -1, -1L, -1, -1);
    CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
    Future<PubSubProduceResult> produceResultFuture = new ApacheKafkaProduceResultFuture(completableFuture);
    assertEquals(produceResultFuture.isDone(), completableFuture.isDone());
    assertEquals(produceResultFuture.isCancelled(), completableFuture.isCancelled());

    completableFuture.complete(recordMetadata); // complete inner future

    assertEquals(produceResultFuture.isDone(), completableFuture.isDone());
    assertEquals(produceResultFuture.isCancelled(), completableFuture.isCancelled());
    assertTrue(produceResultFuture.isDone());
    assertFalse(produceResultFuture.isCancelled());
    PubSubProduceResult produceResult = produceResultFuture.get();
    assertNotNull(produceResult);
    assertEquals(produceResult.getTopic(), recordMetadata.topic());
  }

  @Test(expectedExceptions = CancellationException.class)
  public void testApacheKafkaProduceResultFutureCancel() throws ExecutionException, InterruptedException {
    CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
    Future<PubSubProduceResult> produceResultFuture = new ApacheKafkaProduceResultFuture(completableFuture);
    assertEquals(produceResultFuture.isDone(), completableFuture.isDone());
    assertEquals(produceResultFuture.isCancelled(), completableFuture.isCancelled());

    produceResultFuture.cancel(true); // should invoke cancel inner future

    assertEquals(produceResultFuture.isDone(), completableFuture.isDone());
    assertEquals(produceResultFuture.isCancelled(), completableFuture.isCancelled());
    assertTrue(produceResultFuture.isDone());
    assertTrue(produceResultFuture.isCancelled());

    produceResultFuture.get(); // trying to get value from cancelled future should throw CancellationException
  }

  @Test(expectedExceptions = TimeoutException.class)
  public void testApacheKafkaProduceResultHonorsTimeoutOnGet()
      throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
    Future<PubSubProduceResult> produceResultFuture = new ApacheKafkaProduceResultFuture(completableFuture);
    assertEquals(produceResultFuture.isDone(), completableFuture.isDone());
    assertEquals(produceResultFuture.isCancelled(), completableFuture.isCancelled());
    produceResultFuture.get(10, TimeUnit.MICROSECONDS);
  }
}
