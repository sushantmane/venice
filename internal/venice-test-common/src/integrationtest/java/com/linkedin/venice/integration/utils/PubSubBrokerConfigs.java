package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.TestMockTime;
import java.util.Collections;
import java.util.Map;


public class PubSubBrokerConfigs {
  private final ZkServerWrapper zkServerWrapper;
  private final TestMockTime mockTime;
  private final String regionName;
  private final String clusterName;

  private final Map<String, String> additionalBrokerConfiguration;

  private PubSubBrokerConfigs(Builder builder) {
    this.zkServerWrapper = builder.zkServerWrapper;
    this.mockTime = builder.mockTime;
    this.additionalBrokerConfiguration = builder.additionalBrokerConfiguration;
    this.regionName = builder.regionName;
    this.clusterName = builder.clusterName;
  }

  public ZkServerWrapper getZkWrapper() {
    return zkServerWrapper;
  }

  public TestMockTime getMockTime() {
    return mockTime;
  }

  public Map<String, String> getAdditionalBrokerConfiguration() {
    return additionalBrokerConfiguration;
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

    private Map<String, String> additionalBrokerConfiguration = Collections.emptyMap();

    public Builder setZkWrapper(ZkServerWrapper zkServerWrapper) {
      this.zkServerWrapper = zkServerWrapper;
      return this;
    }

    public Builder setMockTime(TestMockTime mockTime) {
      this.mockTime = mockTime;
      return this;
    }

    public Builder setAdditionalBrokerConfiguration(Map<String, String> additionalBrokerConfiguration) {
      this.additionalBrokerConfiguration = Collections.unmodifiableMap(additionalBrokerConfiguration);
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
