package com.linkedin.venice.controllerapi.request;

/**
 * Extend this class to create request objects for the controller
 */
public class ControllerRequest {
  private final String clusterName;
  private final String storeName;

  public ControllerRequest(String clusterName) {
    this(clusterName, null);
  }

  public ControllerRequest(String clusterName, String storeName) {
    this.clusterName = clusterName;
    this.storeName = storeName;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getStoreName() {
    return storeName;
  }
}
