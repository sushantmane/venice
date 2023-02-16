package com.linkedin.venice.unit.kafka.producer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.SimpleProduceResultImpl;
import com.linkedin.venice.pubsub.api.PubsubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubsubProduceResult;
import com.linkedin.venice.pubsub.api.PubsubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubsubProducerCallback;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.InMemoryKafkaMessage;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * A {@link PubsubProducerAdapter} implementation which interacts with the
 * {@link InMemoryKafkaBroker} in order to make unit tests more lightweight.
 */
public class MockInMemoryProducerAdapter implements PubsubProducerAdapter {
  private final InMemoryKafkaBroker broker;

  public MockInMemoryProducerAdapter(InMemoryKafkaBroker broker) {
    this.broker = broker;
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return broker.getPartitionCount(topic);
  }

  @Override
  public Future<PubsubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubsubMessageHeaders headers,
      PubsubProducerCallback callback) {
    long offset = broker.produce(topic, partition, new InMemoryKafkaMessage(key, value));
    PubsubProduceResult produceResult = new SimpleProduceResultImpl(topic, partition, offset, -1, -1);
    callback.onCompletion(produceResult, null);
    return new Future<PubsubProduceResult>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public PubsubProduceResult get() throws InterruptedException, ExecutionException {
        return produceResult;
      }

      @Override
      public PubsubProduceResult get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return produceResult;
      }
    };
  }

  @Override
  public void flush() {
    // no-op
  }

  @Override
  public void close(int closeTimeOutMs) {
    // no-op
  }

  @Override
  public Map<String, Double> getMeasurableProducerMetrics() {
    return Collections.emptyMap();
  }

  @Override
  public String getBrokerAddress() {
    return broker.getKafkaBootstrapServer();
  }
}
