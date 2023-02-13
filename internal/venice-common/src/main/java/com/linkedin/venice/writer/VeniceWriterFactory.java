package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.ProducerAdapterFactory;
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
  private final ProducerAdapterFactory producerAdapterFactory;

  public VeniceWriterFactory(Properties properties) {
    this(properties, null, null);
  }

  public VeniceWriterFactory(
      Properties properties,
      ProducerAdapterFactory producerAdapterFactory,
      MetricsRepository metricsRepository) {
    this.properties = properties;
    if (metricsRepository != null) {
      new VeniceWriterStats(metricsRepository);
    }
    // For now, if VeniceWriterFactory caller does not pass ProducerAdapterFactory, use Kafka factory as default.
    // Eventually we'll force VeniceWriterFactory creators to inject ProducerAdapterFactory.
    if (producerAdapterFactory == null) {
      producerAdapterFactory = new ApacheKafkaProducerAdapterFactory();
    }
    this.producerAdapterFactory = producerAdapterFactory;
  }

  public <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(VeniceWriterOptions options) {
    Properties writerProperties = new Properties();
    writerProperties.putAll(this.properties);
    // TODO: extract chunking settings directly from VeniceWriterOptions in VeniceWriter
    writerProperties.put(VeniceWriter.ENABLE_CHUNKING, options.isChunkingEnabled());
    writerProperties.put(VeniceWriter.ENABLE_RMD_CHUNKING, options.isRmdChunkingEnabled());
    VeniceProperties props = new VeniceProperties(writerProperties);
    return new VeniceWriter<>(
        options,
        props,
        producerAdapterFactory.create(props, options.getTopicName(), options.getBrokerAddress()));
  }

  /*
   * Marking these classes as deprecated as they are used in other projects and may break them.
   * Please DO NOT use the following deprecated methods. Instead, we should construct VeniceWriterOptions
   * object and pass it to createVeniceWriter(VeniceWriterOptions options).
   *
   * If you happen to change the code in a file where Deprecated createBasicVeniceWriter/createVeniceWriter is used
   * please replace it with the code found in the deprecated method: createVeniceWriter(VeniceWriterOptions)}.
   *
   * The following deprecated methods will be deleted once all clients are upgraded the release containing
   * the new code.
   */

  /** test-only */
  @Deprecated
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName) {
    return createVeniceWriter(new VeniceWriterOptions.Builder(topicName).build());
  }

  @Deprecated
  public <K, V> VeniceWriter<K, V, byte[]> createVeniceWriter(
      String topicName,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topicName).setKeySerializer(keySerializer)
        .setValueSerializer(valueSerializer)
        .build();
    return createVeniceWriter(options);
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
