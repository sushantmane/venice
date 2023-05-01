package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.TestMockTime;


public class PubSubBrokerConfigs {
  private final ZkServerWrapper zkServerWrapper;
  private final TestMockTime mockTime;
  private final String regionName;
  private final String clusterName;

  private PubSubBrokerConfigs(Builder builder) {
    this.zkServerWrapper = builder.zkServerWrapper;
    this.mockTime = builder.mockTime;
    this.regionName = builder.regionName;
    this.clusterName = builder.clusterName;
  }

  public ZkServerWrapper getZkWrapper() {
    return zkServerWrapper;
  }

  public TestMockTime getMockTime() {
    return mockTime;
  }

  public String getRegionName() {
    return regionName;
  }

  public String getClusterName() {
    return clusterName;
  }

  public static class Builder {
    private ZkServerWrapper zkServerWrapper;
    private TestMockTime mockTime;
    private String regionName;
    private String clusterName;

    public Builder setZkWrapper(ZkServerWrapper zkServerWrapper) {
      this.zkServerWrapper = zkServerWrapper;
      return this;
    }

    public Builder setMockTime(TestMockTime mockTime) {
      this.mockTime = mockTime;
      return this;
    }

    public Builder setRegionName(String regionName) {
      this.regionName = regionName;
      return this;
    }

    public Builder setClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public PubSubBrokerConfigs build() {
      return new PubSubBrokerConfigs(this);
    }
  }
}
