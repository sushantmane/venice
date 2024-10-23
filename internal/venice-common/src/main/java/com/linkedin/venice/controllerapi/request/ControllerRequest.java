package com.linkedin.venice.controllerapi.request;

import static java.util.Objects.requireNonNull;


/**
 * Base class for request objects used in controller endpoints.
 *
 * Extend this class to ensure required parameters are validated in the constructor of the extending class.
 * This class is intended for use on both the client and server sides.
 * All required parameters should be passed to and validated within the constructor of the extending class.
 */
public class ControllerRequest {
  private final String clusterName;
  private final String storeName;

  public ControllerRequest(String clusterName) {
    this.clusterName = requireNonNull(clusterName, "Cluster name is mandatory for creating a store");
    this.storeName = null;
  }

  public ControllerRequest(String clusterName, String storeName) {
    this.clusterName = requireNonNull(clusterName, "Cluster name is mandatory for updating ACL for a store");
    this.storeName = requireNonNull(storeName, "Store name is mandatory for updating ACL for a store");
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getStoreName() {
    return storeName;
  }
}
