package com.linkedin.venice.utils;

import static org.testng.Assert.*;

import java.util.Map;
import java.util.Properties;
import org.testng.annotations.Test;


public class VenicePropertiesTest {
  @Test
  public void test() {
    Properties properties = new Properties();
    properties.put("xc.region.to.pubsub.broker.map", "prod:https://prod-broker:1234,dev:dev-broker:1234");
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    Map<String, String> map = veniceProperties.getMap("xc.region.to.pubsub.broker.map");

    System.out.println(map);
    System.out.println(veniceProperties);
  }
}
