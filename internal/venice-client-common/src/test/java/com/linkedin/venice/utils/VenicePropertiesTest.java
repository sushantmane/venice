package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;

import java.util.Map;
import java.util.Properties;
import org.testng.annotations.Test;


public class VenicePropertiesTest {
  @Test
  public void testGetMapWhenMapIsStringEncoded() {
    Properties properties = new Properties();
    properties.put("region.to.pubsub.broker.map", "prod:https://prod-broker:1234,dev:dev-broker:9876");
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    Map<String, String> map = veniceProperties.getMap("region.to.pubsub.broker.map");
    assertEquals(map.size(), 2);
    assertEquals(map.get("prod"), "https://prod-broker:1234");
    assertEquals(map.get("dev"), "dev-broker:9876");
  }
}
