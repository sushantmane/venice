package com.linkedin.venice.pubsub;

import static org.testng.Assert.*;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import org.testng.annotations.Test;


public class PubSubPositionTypeRegistryTest {
  @Test
  public void testGetPositionType() {
    System.out.println(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_ID_TO_CLASS_NAME_MAP);
    PubSubPositionTypeRegistry pubSubPositionTypeRegistry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;
    assertEquals(
        pubSubPositionTypeRegistry.getTypeId(PubSubPosition.LATEST),
        PubSubPositionTypeRegistry.LATEST_POSITION_RESERVED_TYPE_ID);
    assertEquals(
        pubSubPositionTypeRegistry.getTypeId(PubSubPosition.EARLIEST),
        PubSubPositionTypeRegistry.EARLIEST_POSITION_RESERVED_TYPE_ID);
    // com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition
    assertEquals(
        pubSubPositionTypeRegistry
            .getTypeId("com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition"),
        PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID);
  }

}
