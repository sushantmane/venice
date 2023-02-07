package com.linkedin.venice.unit.kafka.producer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.pubsub.api.ProduceResult;
import com.linkedin.venice.pubsub.api.ProducerAdapter;
import com.linkedin.venice.pubsub.api.PubsubProducerCallback;
import com.linkedin.venice.pubsub.protocol.message.KafkaKey;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * This {@link ProducerAdapter} implementation allows tests to perform
 * arbitrary transformations on the messages that are about to be written to
 * Kafka.
 *
 * This can be used in unit tests to inject corrupt data.
 */
public class TransformingProducerAdapter implements ProducerAdapter {
  private final ProducerAdapter baseProducer;
  private final SendMessageParametersTransformer transformer;

  public TransformingProducerAdapter(ProducerAdapter baseProducer, SendMessageParametersTransformer transformer) {
    this.baseProducer = baseProducer;
    this.transformer = transformer;
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return baseProducer.getNumberOfPartitions(topic);
  }

  @Override
  public Future<ProduceResult> sendMessage(
      String topic,
      KafkaKey key,
      KafkaMessageEnvelope value,
      Integer partition,
      PubsubProducerCallback callback) {
    SendMessageParameters parameters = transformer.transform(topic, key, value, partition);
    return baseProducer.sendMessage(parameters.topic, parameters.key, parameters.value, parameters.partition, callback);
  }

  @Override
  public Future<ProduceResult> sendMessage(
      ProducerRecord<KafkaKey, KafkaMessageEnvelope> record,
      PubsubProducerCallback callback) {
    return sendMessage(record.topic(), record.key(), record.value(), record.partition(), callback);
  }

  @Override
  public void flush() {
    baseProducer.flush();
  }

  @Override
  public void close(int closeTimeOutMs) {
    baseProducer.close(closeTimeOutMs);
  }

  @Override
  public Map<String, Double> getMeasurableProducerMetrics() {
    return baseProducer.getMeasurableProducerMetrics();
  }

  public static class SendMessageParameters {
    public final String topic;
    public final KafkaKey key;
    public final KafkaMessageEnvelope value;
    public final int partition;

    public SendMessageParameters(String topic, KafkaKey key, KafkaMessageEnvelope value, int partition) {
      this.topic = topic;
      this.key = key;
      this.value = value;
      this.partition = partition;
    }
  }

  public interface SendMessageParametersTransformer {
    SendMessageParameters transform(String topicName, KafkaKey key, KafkaMessageEnvelope value, int partition);
  }
}
