package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.ControllerEndpointParamValidator;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAclForStoreRequest;
import com.linkedin.venice.meta.Instance;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The core handler for processing incoming requests in the VeniceController.
 * Acts as the central entry point for handling requests received via both HTTP/REST and gRPC protocols.
 * This class is responsible for managing all request handling operations for the VeniceController.
 */
public class VeniceControllerRequestHandler {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerGrpcServiceImpl.class);
  private final Admin admin;
  private final boolean sslEnabled;

  public static final String DEFAULT_STORE_OWNER = "";

  public VeniceControllerRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
    this.sslEnabled = dependencies.isSslEnabled();
  }

  /**
   * Creates a new store in the specified Venice cluster with the provided parameters.
   * @param request the request object containing all necessary details for the creation of the store
   */
  public void createStore(NewStoreRequest request, NewStoreResponse response) {
    ControllerEndpointParamValidator.validateNewStoreRequest(request);

    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    String keySchema = request.getKeySchema();
    String valueSchema = request.getValueSchema();
    String owner = request.getOwner() == null ? DEFAULT_STORE_OWNER : request.getOwner();
    Optional<String> accessPermissions = Optional.ofNullable(request.getAccessPermissions());
    boolean isSystemStore = request.isSystemStore();

    LOGGER.info(
        "Creating store: {} in cluster: {} with owner: {} and key schema: {} and value schema: {} and isSystemStore: {} and access permissions: {}",
        storeName,
        clusterName,
        owner,
        keySchema,
        valueSchema,
        isSystemStore,
        accessPermissions);

    admin.createStore(clusterName, storeName, owner, keySchema, valueSchema, isSystemStore, accessPermissions);

    response.setCluster(clusterName);
    response.setName(storeName);
    response.setOwner(owner);
    LOGGER.info("Successfully created store: {} in cluster: {}", storeName, clusterName);
  }

  public void updateAclForStore(UpdateAclForStoreRequest request, AclResponse response) {
    String cluster = request.getClusterName();
    String storeName = request.getStoreName();
    String accessPermissions = request.getAccessPermissions();
    LOGGER.info(
        "Updating ACL for store: {} in cluster: {} with access permissions: {}",
        storeName,
        cluster,
        accessPermissions);
    admin.updateAclForStore(cluster, storeName, accessPermissions);
    LOGGER.info("Successfully updated ACL for store: {} in cluster: {}", storeName, cluster);
    response.setCluster(cluster);
    response.setName(storeName);
  }

  public void getLeaderController(String clusterName, LeaderControllerResponse response) {
    Instance leaderController = admin.getLeaderController(clusterName);
    response.setCluster(clusterName);
    response.setUrl(leaderController.getUrl(isSslEnabled()));
    if (leaderController.getPort() != leaderController.getSslPort()) {
      // Controller is SSL Enabled
      response.setSecureUrl(leaderController.getUrl(true));
    }
    response.setGrpcUrl(leaderController.getGrpcUrl());
    response.setSecureGrpcUrl(leaderController.getGrpcSslUrl());
  }

  // visibility: package-private
  boolean isSslEnabled() {
    return sslEnabled;
  }
}
