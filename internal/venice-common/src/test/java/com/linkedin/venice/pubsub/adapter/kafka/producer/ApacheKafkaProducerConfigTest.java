package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.annotations.Test;


public class ApacheKafkaProducerConfigTest {
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".* Required property: kafka.bootstrap.servers is missing.*")
  public void testConfiguratorThrowsAnExceptionWhenBrokerAddressIsMissing() {
    VeniceProperties veniceProperties = new VeniceProperties();
    new ApacheKafkaProducerConfig(veniceProperties, null, true);
  }
}
