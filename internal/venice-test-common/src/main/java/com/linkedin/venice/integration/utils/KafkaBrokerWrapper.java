package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.MockTime;
import java.io.File;
import java.util.Optional;


public abstract class KafkaBrokerWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "PubSub";

  KafkaBrokerWrapper(String serviceName, File dataDirectory) {
    super(serviceName, dataDirectory);
  }

  private static final boolean useRedpanda = true;

  /**
   * This is package private because the only way to call this should be from {@link ServiceFactory#getKafkaBroker()}.
   *
   * @return a function which yields a {@link KafkaBrokerWrapper} instance
   */
  static StatefulServiceProvider<KafkaBrokerWrapper> generateService(
      ZkServerWrapper zkServerWrapper,
      Optional<MockTime> mockTime) {
    if (useRedpanda) {
      return RedpandaBrokerWrapper.generateService(zkServerWrapper, mockTime);
    }

    return LiKafkaBrokerWrapper.generateService(zkServerWrapper, mockTime);
  }

  /**
   * @see {@link ProcessWrapper#getHost()}
   */
  public abstract String getHost();

  /**
   * @see {@link ProcessWrapper#getPort()}
   */
  public abstract int getPort();

  public abstract int getSslPort();

  public abstract String getAddress();

  public abstract String getSSLAddress();

  /**
   * @return the address of the ZK used by this Kafka instance
   */
  public abstract String getZkAddress();

  public abstract String toString();
}
