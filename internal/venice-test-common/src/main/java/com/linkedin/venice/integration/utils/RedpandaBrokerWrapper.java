package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.MockTime;
import java.io.File;
import java.util.Optional;
import org.testcontainers.redpanda.RedpandaContainer;


public class RedpandaBrokerWrapper extends KafkaBrokerWrapper {
  private static final String REDPANDA_IMAGE = "docker.redpanda.com/vectorized/redpanda:v22.2.1";
  private final RedpandaContainer redpandaContainer;
  private final ZkServerWrapper zkServerWrapper;
  private static final int REDPANDA_PORT = 9092;
  public static final String SERVICE_NAME = "PubSubRedPanda";

  RedpandaBrokerWrapper(String serviceName, File dataDirectory, ZkServerWrapper zkServerWrapper) {
    super(serviceName, dataDirectory);
    this.zkServerWrapper = zkServerWrapper;
    this.redpandaContainer = new RedpandaContainer(REDPANDA_IMAGE);
  }

  static StatefulServiceProvider<KafkaBrokerWrapper> generateService(
      ZkServerWrapper zkServerWrapper,
      Optional<MockTime> mockTime) {
    return (String serviceName, File dir) -> new RedpandaBrokerWrapper(serviceName, dir, zkServerWrapper);
  }

  @Override
  protected void internalStart() throws Exception {
    redpandaContainer.start();
  }

  @Override
  protected void internalStop() throws Exception {
    redpandaContainer.stop();
  }

  @Override
  protected void newProcess() throws Exception {
  }

  @Override
  public String getHost() {
    return redpandaContainer.getHost();
  }

  @Override
  public int getPort() {
    return redpandaContainer.getMappedPort(REDPANDA_PORT);
  }

  @Override
  public int getSslPort() {
    return redpandaContainer.getMappedPort(REDPANDA_PORT);
  }

  @Override
  public String getAddress() {
    return getHost() + ":" + getPort();
  }

  @Override
  public String getSSLAddress() {
    return getHost() + ":" + getSslPort();
  }

  @Override
  public String getZkAddress() {
    return zkServerWrapper.getAddress();
  }

  @Override
  public String toString() {
    return "RedpandaWrapper{address: '" + getAddress() + "', sslAddress: '" + getSSLAddress() + "'}";
  }

  public static void main(String[] args) {
    RedpandaContainer redpandaContainer = new RedpandaContainer(REDPANDA_IMAGE);
    redpandaContainer.start();

    System.out.println(redpandaContainer);

    redpandaContainer.stop();
  }
}
