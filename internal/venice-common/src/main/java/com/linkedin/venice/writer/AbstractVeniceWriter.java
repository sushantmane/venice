package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.ProduceResult;
import com.linkedin.venice.pubsub.api.PubsubProducerCallback;
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

  public Future<ProduceResult> put(K key, V value, int valueSchemaId) {
    return put(key, value, valueSchemaId, null);
  }

  public abstract void close(boolean gracefulClose) throws IOException;

  public abstract Future<ProduceResult> put(K key, V value, int valueSchemaId, PubsubProducerCallback callback);

  public abstract Future<ProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubsubProducerCallback callback,
      PutMetadata putMetadata);

  public abstract Future<ProduceResult> delete(K key, PubsubProducerCallback callback, DeleteMetadata deleteMetadata);

  public abstract Future<ProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubsubProducerCallback callback);

  public abstract void flush();
}
