package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubsubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubsubProduceResult;
import com.linkedin.venice.pubsub.api.PubsubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubsubProducerCallback;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Implementation of the shared Kafka Producer for sending messages to multiple Kafka topics concurrently.
 */
public class SharedKafkaProducerAdapter implements PubsubProducerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(SharedKafkaProducerAdapter.class);

  private final SharedKafkaProducerAdapterFactory sharedKafkaProducerAdapterFactory;
  private final int id;
  private final Set<String> producerTasks;
  private final PubsubProducerAdapter producerAdapter;

  private long lastStatUpdateTsMs = 0;
  private final Map<String, Double> kafkaProducerMetrics;
  private SharedKafkaProducerStats sharedKafkaProducerStats;

  public SharedKafkaProducerAdapter(
      SharedKafkaProducerAdapterFactory sharedKafkaProducerAdapterFactory,
      int id,
      PubsubProducerAdapter producerAdapter,
      MetricsRepository metricsRepository,
      Set<String> metricsToBeReported) {
    this.sharedKafkaProducerAdapterFactory = sharedKafkaProducerAdapterFactory;
    this.id = id;
    producerTasks = new HashSet<>();
    this.producerAdapter = producerAdapter;
    kafkaProducerMetrics = new HashMap<>();
    metricsToBeReported
        .forEach(metric -> kafkaProducerMetrics.put(metric, (double) StatsErrorCode.KAFKA_CLIENT_METRICS_DEFAULT.code));
    if (metricsRepository != null && kafkaProducerMetrics.size() > 0) {
      sharedKafkaProducerStats = new SharedKafkaProducerStats(metricsRepository);
    }
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return producerAdapter.getNumberOfPartitions(topic);
  }

  /**
   * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
   * @param topic - The topic to be sent to.
   * @param key - The key of the message to be sent.
   * @param value - The {@link KafkaMessageEnvelope}, which acts as the Kafka value.
   * @param callback - The callback function, which will be triggered when Kafka client sends out the message.
   * */
  @Override
  public Future<PubsubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubsubMessageHeaders headers,
      PubsubProducerCallback callback) {
    long startNs = System.nanoTime();
    Future<PubsubProduceResult> result = producerAdapter.sendMessage(topic, partition, key, value, headers, callback);
    sharedKafkaProducerStats.recordProducerSendLatency(LatencyUtils.getLatencyInMS(startNs));
    return result;
  }

  @Override
  public void flush() {
    producerAdapter.flush();
  }

  @Override
  public void close(int closeTimeOutMs) {
    producerAdapter.close(closeTimeOutMs);
  }

  @Override
  public void close(int closeTimeOutMs, boolean doFlush) {
    producerAdapter.close(closeTimeOutMs, doFlush);
  }

  @Override
  public void close(String topic, int closeTimeoutMs) {
    if (sharedKafkaProducerAdapterFactory.isRunning()) {
      sharedKafkaProducerAdapterFactory.releaseKafkaProducer(topic);
    } else {
      LOGGER.info("producer is already closed, can't release for topic: {}", topic);
    }
  }

  @Override
  public void close(String topic, int closeTimeoutMs, boolean doFlush) {
    if (doFlush) {
      producerAdapter.flush();
    }
    close(topic, closeTimeoutMs);
  }

  @Override
  public Map<String, Double> getMeasurableProducerMetrics() {
    return producerAdapter.getMeasurableProducerMetrics();
  }

  @Override
  public String getBrokerAddress() {
    return producerAdapter.getBrokerAddress();
  }

  public int getId() {
    return id;
  }

  public synchronized void addProducerTask(String producerTaskName) {
    producerTasks.add(producerTaskName);
  }

  public synchronized void removeProducerTask(String producerTaskName) {
    producerTasks.remove(producerTaskName);
  }

  public int getProducerTaskCount() {
    return producerTasks.size();
  }

  public String toString() {
    return "{Id: " + id + ", Task Count: " + getProducerTaskCount() + "}";
  }

  /**
   * Stats related api's.
   *
   * Following are the list of metrics available from a KafkaProducer client.
   *
   * connection-creation-total
   * bufferpool-wait-time-total
   * batch-split-total
   * select-rate
   * produce-throttle-time-max
   * connection-close-total
   * byte-total
   * successful-reauthentication-rate
   * outgoing-byte-rate
   * record-send-total
   * batch-size-max
   * compression-rate
   * failed-reauthentication-rate
   * produce-throttle-time-avg
   * iotime-total
   * successful-authentication-total
   * successful-authentication-no-reauth-total
   * batch-split-rate
   * count
   * io-waittime-total
   * failed-reauthentication-total
   * request-rate
   * buffer-available-bytes
   * outgoing-byte-total
   * buffer-exhausted-total
   * buffer-exhausted-rate
   * record-send-rate
   * response-rate
   * record-queue-time-avg
   * metadata-age
   * network-io-rate
   * io-ratio
   * request-total
   * io-wait-ratio
   * request-size-max
   * successful-authentication-rate
   * failed-authentication-rate
   * network-io-total
   * record-queue-time-max
   * incoming-byte-total
   * response-total
   * incoming-byte-rate
   * waiting-threads
   * bufferpool-wait-ratio
   * connection-close-rate
   * request-size-avg
   * records-per-request-avg
   * connection-creation-rate
   * record-size-avg
   * record-retry-total
   * record-error-total
   * request-latency-avg
   * connection-count
   * io-wait-time-ns-avg
   * record-error-rate
   * requests-in-flight
   * reauthentication-latency-max
   * failed-authentication-total
   * io-time-ns-avg
   * compression-rate-avg
   * record-retry-rate
   * request-latency-max
   * record-size-max
   * select-total
   * byte-rate
   * successful-reauthentication-total
   * buffer-total-bytes
   * batch-size-avg
   * reauthentication-latency-avg
   *
   *
   * Currently we are reporting the following ones which may be interesting and useful to tune the producers config.
   *
   *  outgoing-byte-rate :
   * 	record-send-rate :
   * 	batch-size-max :
   * 	batch-size-avg :
   * 	buffer-available-bytes :
   * 	buffer-exhausted-rate :
   */
  private synchronized void mayBeCalculateAllProducerMetrics() {
    if (LatencyUtils.getElapsedTimeInMs(lastStatUpdateTsMs) < 60 * Time.MS_PER_SECOND) {
      return;
    }

    // measure
    Map<String, Double> metrics = producerAdapter.getMeasurableProducerMetrics();
    for (String metricName: kafkaProducerMetrics.keySet()) {
      kafkaProducerMetrics
          .put(metricName, metrics.getOrDefault(metricName, (double) StatsErrorCode.KAFKA_CLIENT_METRICS_DEFAULT.code));
    }

    lastStatUpdateTsMs = System.currentTimeMillis();
  }

  private class SharedKafkaProducerStats extends AbstractVeniceStats {
    private final Sensor producerSendLatencySensor;

    public SharedKafkaProducerStats(MetricsRepository metricsRepository) {
      super(metricsRepository, "SharedKafkaProducerAdapter");
      kafkaProducerMetrics.keySet().forEach(metric -> {
        String metricName = "producer_" + id + "_" + metric;
        LOGGER.info("SharedKafkaProducerAdapter: Registering metric: {}", metricName);
        registerSensorIfAbsent(metricName, new Gauge(() -> {
          mayBeCalculateAllProducerMetrics();
          return kafkaProducerMetrics.get(metric);
        }));
      });
      producerSendLatencySensor = registerSensor("producer_" + id + "_send_api_latency", new Avg(), new Max());
    }

    public void recordProducerSendLatency(double value) {
      producerSendLatencySensor.record(value);
    }
  }
}
