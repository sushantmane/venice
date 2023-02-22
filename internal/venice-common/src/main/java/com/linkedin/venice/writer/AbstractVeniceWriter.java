package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;


/**
 * A base class which users of {@link VeniceWriter} can leverage in order to
 * make unit tests easier.
 *
 * @see VeniceWriter
 * // @see MockVeniceWriter in the VPJ tests (commented because this module does not depend on VPJ)
 */
public abstract class AbstractVeniceWriter<K, V, U> implements Closeable {
  protected final String topicName;

  public AbstractVeniceWriter(String topicName) {
    this.topicName = topicName;
  }

  public String getTopicName() {
    return this.topicName;
  }

  public Future<PubSubProduceResult> put(K key, V value, int valueSchemaId) {
    return putSync(key, value, valueSchemaId, null);
  }

  public abstract void close(boolean gracefulClose) throws IOException;

  public abstract void put(K key, V value, int valueSchemaId, PubSubProducerCallback callback);

  public abstract Future<PubSubProduceResult> putSync(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback);

  public abstract void put(K key, V value, int valueSchemaId, PubSubProducerCallback callback, PutMetadata putMetadata);

  public abstract void delete(K key, PubSubProducerCallback callback, DeleteMetadata deleteMetadata);

  public abstract void update(K key, U update, int valueSchemaId, int derivedSchemaId, PubSubProducerCallback callback);

  public abstract void flush();
}
