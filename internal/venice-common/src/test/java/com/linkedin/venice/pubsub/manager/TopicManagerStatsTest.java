package com.linkedin.venice.pubsub.manager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.EnumMap;
import org.testng.annotations.Test;


public class TopicManagerStatsTest {
  @Test
  public void testRecordLatencyInteractions() {
    // Test: when topicManagerStats is null, no exception is thrown.
    TopicManagerStats.recordLatency(null, TopicManagerStats.SENSOR_TYPE.CONTAINS_TOPIC, 100);

    // Test: when sensorType is null, no exception is thrown.
    TopicManagerStats topicManagerStatsMock = mock(TopicManagerStats.class);
    TopicManagerStats.recordLatency(topicManagerStatsMock, null, 100);

    // Test: when topicManagerStats and sensorType are not null, no exception is thrown.
    EnumMap<TopicManagerStats.SENSOR_TYPE, Sensor> sensorsByTypes = new EnumMap<>(TopicManagerStats.SENSOR_TYPE.class);
    Sensor sensorMock = mock(Sensor.class);
    sensorsByTypes.put(TopicManagerStats.SENSOR_TYPE.CONTAINS_TOPIC, sensorMock);
    when(topicManagerStatsMock.getSensorsByTypes()).thenReturn(sensorsByTypes);
    TopicManagerStats.recordLatency(topicManagerStatsMock, TopicManagerStats.SENSOR_TYPE.CONTAINS_TOPIC, 100);
    verify(sensorMock).record(100);

    // Test: when sensor is null, no exception is thrown.
    sensorsByTypes.put(TopicManagerStats.SENSOR_TYPE.CONTAINS_TOPIC, null);
    TopicManagerStats.recordLatency(topicManagerStatsMock, TopicManagerStats.SENSOR_TYPE.CONTAINS_TOPIC, 100);
  }

  @Test
  public void testRecordLatency() {
    MetricsRepository metricsRepository = new MetricsRepository();
    String pubSubClusterAddress = "venice.kafka.dc-1.linkedin.com:12345";
    TopicManagerStats topicManagerStats = new TopicManagerStats(metricsRepository, pubSubClusterAddress);
    EnumMap<TopicManagerStats.SENSOR_TYPE, Sensor> sensorsByTypes = topicManagerStats.getSensorsByTypes();
    assertNotNull(sensorsByTypes);
    assertEquals(sensorsByTypes.size(), TopicManagerStats.SENSOR_TYPE.values().length);
    assertEquals(topicManagerStats.getMetricsRepository(), metricsRepository);
    assertEquals(topicManagerStats.getName(), ".TopicManagerStats_venice_kafka_dc-1_linkedin_com_12345");
  }
}
