package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils;
import com.linkedin.venice.pubsub.api.ProduceResult;
import com.linkedin.venice.pubsub.api.ProducerAdapter;
import com.linkedin.venice.pubsub.api.PubsubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubsubProducerCallback;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Implementation of the Kafka Producer for sending messages to Kafka.
 */
public class ApacheKafkaProducerAdapter implements ProducerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerAdapter.class);

  private KafkaProducer<KafkaKey, KafkaMessageEnvelope> producer;

  /**
   * @param cfg contains producer configs
   */
  public ApacheKafkaProducerAdapter(ApacheKafkaProducerConfig cfg) {
    LOGGER.info("Constructing KafkaProducer with the following properties: {}", cfg.getProducerProperties());
    producer = new KafkaProducer<>(cfg.getProducerProperties());
  }

  /**
   * N.B.: This is an expensive call, the result of which should be cached.
   *
   * @param topic for which we want to request the number of partitions.
   * @return the number of partitions for this topic.
   */
  @Deprecated
  public int getNumberOfPartitions(String topic) {
    ensureProducerIsNotClosed();
    // TODO: This blocks forever. Using getNumberOfPartitions with timeout parameter adds a timeout to this call but
    // other usages need to be refactored to handle the timeout exception correctly
    return producer.partitionsFor(topic).size();
  }

  /**
   * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
   * @param topic - The topic to be sent to.
   * @param key - The key of the message to be sent.
   * @param value - The {@link KafkaMessageEnvelope}, which acts as the Kafka value.
   * @param pubsubProducerCallback - The callback function, which will be triggered when Kafka client sends out the message.
   * */
  @Override
  public Future<ProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubsubMessageHeaders pubsubMessageHeaders,
      PubsubProducerCallback pubsubProducerCallback) {
    ensureProducerIsNotClosed();
    ProducerRecord<KafkaKey, KafkaMessageEnvelope> record = new ProducerRecord<>(
        topic,
        partition,
        key,
        value,
        ApacheKafkaUtils.convertToKafkaSpecificHeaders(pubsubMessageHeaders));
    Callback kafkaCallback = null;
    if (pubsubProducerCallback != null) {
      kafkaCallback = new ApacheKafkaProducerCallback(pubsubProducerCallback);
    }
    try {
      // TODO: evaluate if it makes sense to complete Future<ProduceResult> in callback itself or use producer
      // interceptors
      return new ApacheKafkaProduceResultFuture(producer.send(record, kafkaCallback));
    } catch (Exception e) {
      throw new VeniceException(
          "Got an error while trying to produce message into Kafka. Topic: '" + record.topic() + "', partition: "
              + record.partition(),
          e);
    }
  }

  private void ensureProducerIsNotClosed() {
    if (producer == null) {
      throw new VeniceException("The internal KafkaProducer has been closed");
    }
  }

  @Override
  public void flush() {
    if (producer != null) {
      producer.flush();
    }
  }

  @Override
  public void close(int closeTimeOutMs) {
    close(closeTimeOutMs, true);
  }

  @Override
  public void close(String topic, int closeTimeoutMs, boolean doFlush) {
    close(closeTimeoutMs, doFlush);
  }

  @Override
  public void close(int closeTimeOutMs, boolean doFlush) {
    if (producer != null) {
      if (doFlush) {
        // Flush out all the messages in the producer buffer
        producer.flush(closeTimeOutMs, TimeUnit.MILLISECONDS);
        LOGGER.info("Flushed all the messages in producer before closing");
      }
      producer.close(Duration.ofMillis(closeTimeOutMs));
      // Recycle the internal buffer allocated by KafkaProducer ASAP.
      producer = null;
    }
  }

  @Override
  public Map<String, Double> getMeasurableProducerMetrics() {
    if (producer == null) {
      return Collections.emptyMap();
    }
    Map<String, Double> extractedMetrics = new HashMap<>();
    for (Map.Entry<MetricName, ? extends Metric> entry: producer.metrics().entrySet()) {
      try {
        Object value = entry.getValue().metricValue();
        if (value instanceof Double) {
          extractedMetrics.put(entry.getKey().name(), (Double) value);
        }
      } catch (Exception e) {
        LOGGER.warn(
            "Caught exception: {} when attempting to get producer metrics. Incomplete metrics might be returned.",
            e.getMessage());
      }
    }
    return extractedMetrics;
  }
}
