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
  private static final String MISSING_CLUSTER_NAME = "Cluster name is missing in the request. It is mandatory";
  private static final String MISSING_STORE_NAME = "Store name is missing in the request. It is mandatory";

  private final String clusterName;
  private final String storeName;

  public ControllerRequest(String clusterName) {
    this.clusterName = requireNonNull(clusterName, MISSING_CLUSTER_NAME);
    this.storeName = null;
  }

  public ControllerRequest(String clusterName, String storeName) {
    this.clusterName = requireNonNull(clusterName, MISSING_CLUSTER_NAME);
    this.storeName = requireNonNull(storeName, MISSING_STORE_NAME);
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getStoreName() {
    return storeName;
  }
}
