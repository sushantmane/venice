package com.linkedin.venice.fastclient.request;

public class FcComputeRequest {
  private String route;
  private String keyString;
  private String resourceName;

  // create builder for FcComputeRequest
  private FcComputeRequest(Builder builder) {
    this.route = builder.route;
    this.keyString = builder.keyString;
    this.resourceName = builder.resourceName;
  }

  public String getRoute() {
    return route;
  }

  public String getKeyString() {
    return keyString;
  }

  public String getResourceName() {
    return resourceName;
  }

  // Builder class for FcComputeRequest
  public static class Builder {
    private String route;
    private String keyString;
    private String resourceName;

    public Builder setRoute(String route) {
      this.route = route;
      return this;
    }

    public Builder setKeyString(String keyString) {
      this.keyString = keyString;
      return this;
    }

    public Builder setResourceName(String resourceName) {
      this.resourceName = resourceName;
      return this;
    }

    public FcComputeRequest build() {
      return new FcComputeRequest(this);
    }
  }
}
