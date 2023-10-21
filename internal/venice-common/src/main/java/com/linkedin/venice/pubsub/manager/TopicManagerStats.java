package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.EnumMap;


/**
 * Stats for topic manager.
 */
public class TopicManagerStats extends AbstractVeniceStats {
  public enum OCCURRENCE_LATENCY_SENSOR_TYPE {
    CREATE_TOPIC, DELETE_TOPIC, LIST_ALL_TOPICS, SET_TOPIC_CONFIG, GET_ALL_TOPIC_RETENTIONS, GET_TOPIC_CONFIG,
    GET_TOPIC_CONFIG_WITH_RETRY, CONTAINS_TOPIC, GET_SOME_TOPIC_CONFIGS, CONTAINS_TOPIC_WITH_RETRY,
    GET_TOPIC_LATEST_OFFSETS, GET_TOPIC_CONFIG_IF_EXISTS, GET_PARTITION_LATEST_OFFSET_WITH_RETRY,
    GET_PARTITIONS_OFFSETS_BY_TIME, GET_PARTITION_OFFSET_BY_TIME,
    GET_LATEST_PRODUCER_TIMESTAMP_ON_DATA_RECORD_WITH_RETRY, PARTITIONS_FOR,
    GET_PARTITION_OFFSET_BY_TIME_IF_OUT_OF_RANGE
  }

  private final EnumMap<OCCURRENCE_LATENCY_SENSOR_TYPE, Sensor> sensorsByTypes;

  public TopicManagerStats(MetricsRepository metricsRepository, String pubSubClusterAddress) {
    super(metricsRepository, "TopicManagerStats_" + TehutiUtils.fixMalformedMetricName(pubSubClusterAddress));
    sensorsByTypes = new EnumMap<>(OCCURRENCE_LATENCY_SENSOR_TYPE.class);
    for (OCCURRENCE_LATENCY_SENSOR_TYPE sensorType: OCCURRENCE_LATENCY_SENSOR_TYPE.values()) {
      final String sensorName = sensorType.name().toLowerCase();
      sensorsByTypes.put(
          sensorType,
          registerSensorIfAbsent(
              sensorName,
              new OccurrenceRate(),
              new Max(),
              new Min(),
              new Avg(),
              TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + sensorName)));
    }
  }

  // Record latency for a specific sensor type if the topic manager stats is not null.
  public static void recordLatency(
      TopicManagerStats topicManagerStats,
      OCCURRENCE_LATENCY_SENSOR_TYPE sensorType,
      long requestLatencyMs) {
    if (topicManagerStats == null || sensorType == null) {
      return;
    }
    Sensor sensor = topicManagerStats.sensorsByTypes.get(sensorType);
    if (sensor == null) {
      return;
    }
    sensor.record(requestLatencyMs);
  }
}
