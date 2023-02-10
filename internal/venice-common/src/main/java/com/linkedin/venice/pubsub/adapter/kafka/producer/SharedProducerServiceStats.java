package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;


public class SharedProducerServiceStats extends AbstractVeniceStats {
  private final SharedKafkaProducerAdapterFactory sharedKafkaProducerAdapterFactory;

  /**
   * Metric to keep track of number of currently active ingestion tasks that is using a shared producer instance.
   */
  private Sensor sharedProducerActiveTasksCountSensor;

  /**
   * Metric to keep track of number of open shared producer instance.
   */
  private Sensor sharedProducerActiveCountSensor;

  public SharedProducerServiceStats(
      MetricsRepository metricsRepository,
      SharedKafkaProducerAdapterFactory sharedKafkaProducerAdapterFactory) {
    super(metricsRepository, "SharedKafkaProducerServiceStats");
    this.sharedKafkaProducerAdapterFactory = sharedKafkaProducerAdapterFactory;
    sharedProducerActiveTasksCountSensor = registerSensor(
        "shared_producer_active_task_count",
        new Gauge(() -> sharedKafkaProducerAdapterFactory.getActiveSharedProducerTasksCount()));
    sharedProducerActiveCountSensor = registerSensor(
        "shared_producer_active_count",
        new Gauge(() -> sharedKafkaProducerAdapterFactory.getActiveSharedProducerCount()));
  }
}
