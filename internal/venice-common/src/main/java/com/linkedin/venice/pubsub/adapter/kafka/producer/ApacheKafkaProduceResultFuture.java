package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * A generic future ({@link Future< PubSubProduceResult >}) wrapper over {@link Future<RecordMetadata>}
 */
public class ApacheKafkaProduceResultFuture implements Future<PubSubProduceResult> {
  private final Future<RecordMetadata> recordMetadataFuture;

  public ApacheKafkaProduceResultFuture(Future<RecordMetadata> recordMetadataFuture) {
    this.recordMetadataFuture = Objects.requireNonNull(recordMetadataFuture, "RecordMetadata future cannot be null");
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return recordMetadataFuture.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return recordMetadataFuture.isCancelled();
  }

  @Override
  public boolean isDone() {
    return recordMetadataFuture.isDone();
  }

  @Override
  public PubSubProduceResult get() throws InterruptedException, ExecutionException {
    return new ApacheKafkaProduceResult(recordMetadataFuture.get());
  }

  @Override
  public PubSubProduceResult get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return new ApacheKafkaProduceResult(recordMetadataFuture.get(timeout, unit));
  }
}
