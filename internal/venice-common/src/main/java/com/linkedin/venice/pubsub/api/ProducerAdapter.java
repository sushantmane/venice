package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public interface ProducerAdapter {
  ExecutorService timeOutExecutor = Executors.newSingleThreadExecutor();

  /**
   * The support for the following two getNumberOfPartitions APIs will be removed.
   */
  @Deprecated
  int getNumberOfPartitions(String topic);

  @Deprecated
  default int getNumberOfPartitions(String topic, int timeout, TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    Callable<Integer> task = () -> getNumberOfPartitions(topic);
    Future<Integer> future = timeOutExecutor.submit(task);
    return future.get(timeout, timeUnit);
  }

  Future<ProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubsubMessageHeaders headers,
      PubsubProducerCallback callback);

  default Future<ProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubsubProducerCallback callback) {
    return sendMessage(topic, partition, key, value, null, callback);
  }

  default Future<ProduceResult> sendMessage(
      String topic,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubsubProducerCallback callback) {
    return sendMessage(topic, null, key, value, null, callback);
  }

  default Future<ProduceResult> sendMessage(String topic, KafkaKey key, KafkaMessageEnvelope value) {
    return sendMessage(topic, null, key, value, null, null);
  }

  void flush();

  void close(int closeTimeOutMs);

  default void close(int closeTimeOutMs, boolean doFlush) {
    close(closeTimeOutMs);
  }

  default void close(String topic, int closeTimeOutMs, boolean doFlush) {
    close(closeTimeOutMs, doFlush);
  }

  default void close(String topic, int closeTimeOutMs) {
    close(closeTimeOutMs, true);
  }

  Map<String, Double> getMeasurableProducerMetrics();
}
