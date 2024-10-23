package com.linkedin.venice.controllerapi.request;

public class UpdateStoreAclRequest extends ControllerRequest {
  private String storeName;
  private String accessPermissions;

  public UpdateStoreAclRequest(String clusterName) {
    super(clusterName);
  }

  public String getStoreName() {
    return storeName;
  }

  public void setStoreName(String storeName) {
    this.storeName = storeName;
  }

  public String getAccessPermissions() {
    return accessPermissions;
  }

  public void setAccessPermissions(String accessPermissions) {
    this.accessPermissions = accessPermissions;
  }
}
