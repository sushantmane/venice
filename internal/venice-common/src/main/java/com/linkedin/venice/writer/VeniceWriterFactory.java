package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubsubProducerAdapterFactory;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.stats.VeniceWriterStats;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;


/**
 * Factory used to create the venice writer.
 */
public class VeniceWriterFactory {
  private final Properties properties;
  private final PubsubProducerAdapterFactory producerAdapterFactory;

  public VeniceWriterFactory(Properties properties) {
    this(properties, null, null);
  }

  public VeniceWriterFactory(
      Properties properties,
      PubsubProducerAdapterFactory producerAdapterFactory,
      MetricsRepository metricsRepository) {
    this.properties = properties;
    if (metricsRepository != null) {
      new VeniceWriterStats(metricsRepository);
    }
    // For now, if VeniceWriterFactory caller does not pass PubsubProducerAdapterFactory, use Kafka factory as default.
    // Eventually we'll force VeniceWriterFactory creators to inject PubsubProducerAdapterFactory.
    if (producerAdapterFactory == null) {
      producerAdapterFactory = new ApacheKafkaProducerAdapterFactory();
    }
    this.producerAdapterFactory = producerAdapterFactory;
  }

  public <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(VeniceWriterOptions options) {
    VeniceProperties props = new VeniceProperties(this.properties);
    return new VeniceWriter<>(
        options,
        props,
        producerAdapterFactory.create(props, options.getTopicName(), options.getBrokerAddress()));
  }

  /** test-only */
  @Deprecated
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName) {
    return createVeniceWriter(new VeniceWriterOptions.Builder(topicName).build());
  }

  /**
   * test-only
   *
   * @param chunkingEnabled override the factory's default chunking setting.
   */
  @Deprecated
  public <K, V> VeniceWriter<K, V, byte[]> createVeniceWriter(
      String topicName,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer,
      boolean chunkingEnabled) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topicName).setKeySerializer(keySerializer)
        .setValueSerializer(valueSerializer)
        .setChunkingEnabled(chunkingEnabled)
        .build();
    return createVeniceWriter(options);
  }
}
