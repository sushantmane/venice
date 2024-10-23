package com.linkedin.venice.controllerapi.request;

import java.util.Objects;


public class UpdateAclForStoreRequest extends ControllerRequest {
  private final String accessPermissions;

  public UpdateAclForStoreRequest(String clusterName, String storeName, String accessPermissions) {
    super(clusterName, storeName);
    this.accessPermissions =
        Objects.requireNonNull(accessPermissions, "Access permissions is mandatory for updating ACL for a store");
  }

  public String getAccessPermissions() {
    return accessPermissions;
  }
}
