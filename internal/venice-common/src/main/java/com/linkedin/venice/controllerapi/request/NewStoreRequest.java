package com.linkedin.venice.controllerapi.request;

import static java.util.Objects.requireNonNull;

import com.linkedin.venice.controllerapi.ControllerEndpointParamValidator;


/**
 * Represents a request to create a new store in the specified Venice cluster with the provided parameters.
 * This class encapsulates all necessary details for the creation of a store, including its name, owner,
 * schema definitions, and access permissions.
 */
public class NewStoreRequest extends ControllerRequest {
  private String owner;
  private String keySchema;
  private String valueSchema;
  private boolean isSystemStore;

  // a JSON string representing the access permissions for the store
  private String accessPermissions;

  public NewStoreRequest(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      String accessPermissions,
      boolean isSystemStore) {
    super(clusterName, storeName);
    this.keySchema = requireNonNull(keySchema, "Key schema is mandatory for creating a store");
    this.valueSchema = requireNonNull(valueSchema, "Value schema is mandatory for creating a store");
    this.owner = owner;
    this.accessPermissions = accessPermissions;
    this.isSystemStore = isSystemStore;
    ControllerEndpointParamValidator.validateNewStoreRequest(this);
  }

  public String getOwner() {
    return owner;
  }

  public String getKeySchema() {
    return keySchema;
  }

  public String getValueSchema() {
    return valueSchema;
  }

  public String getAccessPermissions() {
    return accessPermissions;
  }

  public boolean isSystemStore() {
    return isSystemStore;
  }
}
