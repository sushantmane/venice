package com.linkedin.venice.fastclient.request;

public class FcSingleGetRequest {
  private String route;
  private String keyString;
  private String resourceName;

  // create builder for FcComputeRequest
  private FcSingleGetRequest(FcSingleGetRequest.Builder builder) {
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

  // Builder class for FcSingleGetRequest
  public static class Builder {
    private String route;
    private String keyString;
    private String resourceName;

    public FcSingleGetRequest.Builder setRoute(String route) {
      this.route = route;
      return this;
    }

    public FcSingleGetRequest.Builder setKeyString(String keyString) {
      this.keyString = keyString;
      return this;
    }

    public FcSingleGetRequest.Builder setResourceName(String resourceName) {
      this.resourceName = resourceName;
      return this;
    }

    public FcSingleGetRequest build() {
      return new FcSingleGetRequest(this);
    }
  }
}
