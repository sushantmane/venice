package com.linkedin.venice.unit.kafka.producer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.SimplePubSubProduceResultImpl;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.InMemoryKafkaMessage;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * A {@link PubSubProducerAdapter} implementation which interacts with the
 * {@link InMemoryKafkaBroker} in order to make unit tests more lightweight.
 */
public class MockInMemoryProducerAdapter implements PubSubProducerAdapter {
  private final InMemoryKafkaBroker broker;

  public MockInMemoryProducerAdapter(InMemoryKafkaBroker broker) {
    this.broker = broker;
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return broker.getPartitionCount(topic);
  }

  @Override
  public Future<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubMessageHeaders headers,
      PubSubProducerCallback callback) {
    long offset = broker.produce(topic, partition, new InMemoryKafkaMessage(key, value));
    PubSubProduceResult produceResult = new SimplePubSubProduceResultImpl(topic, partition, offset, -1, -1);
    callback.onCompletion(produceResult, null);
    return new Future<PubSubProduceResult>() {
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
      public PubSubProduceResult get() throws InterruptedException, ExecutionException {
        return produceResult;
      }

      @Override
      public PubSubProduceResult get(long timeout, TimeUnit unit)
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
