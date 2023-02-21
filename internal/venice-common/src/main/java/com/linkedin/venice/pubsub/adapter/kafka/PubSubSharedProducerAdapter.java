package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.venice.pubsub.adapter.kafka.producer.SharedKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class PubSubSharedProducerAdapter implements PubSubProducerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(PubSubSharedProducerAdapter.class);

  private final Set<String> producerTasks = new HashSet<>();
  private final PubSubProducerAdapter producerAdapter;
  private final int id;
  private final SharedKafkaProducerAdapterFactory sharedProducerAdapterFactory;
  private final MetricsRepository metricsRepository;

  public PubSubSharedProducerAdapter(
      int id,
      SharedKafkaProducerAdapterFactory sharedProducerAdapterFactory,
      PubSubProducerAdapter producerAdapter,
      MetricsRepository metricsRepository) {
    this.id = id;
    this.sharedProducerAdapterFactory = sharedProducerAdapterFactory;
    this.producerAdapter = producerAdapter;
    this.metricsRepository = metricsRepository;
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
}
