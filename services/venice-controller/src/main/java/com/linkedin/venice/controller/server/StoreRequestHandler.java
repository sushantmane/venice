package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreRequestHandler {
  Logger LOGGER = LogManager.getLogger(StoreRequestHandler.class);

  private final Admin admin;
  private final boolean sslEnabled;
  private final VeniceControllerAccessManager accessManager;

  public StoreRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
    this.sslEnabled = dependencies.isSslEnabled();
    this.accessManager = dependencies.getControllerAccessManager();
  }

  public UpdateAclForStoreGrpcResponse updateAclForStore(UpdateAclForStoreGrpcRequest request) {
    ControllerRequestParamValidator.validateClusterStoreInfo(request.getStoreInfo());
    String accessPermissions = request.getAccessPermissions();
    if (StringUtils.isBlank(accessPermissions)) {
      throw new IllegalArgumentException("Access permissions is required for updating ACL");
    }
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    String cluster = storeInfo.getClusterName();
    String storeName = storeInfo.getStoreName();
    LOGGER.info(
        "Updating ACL for store: {} in cluster: {} with access permissions: {}",
        storeName,
        cluster,
        accessPermissions);
    admin.updateAclForStore(cluster, storeName, accessPermissions);
    LOGGER.info("Successfully updated ACL for store: {} in cluster: {}", storeName, cluster);
    return UpdateAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo).build();
  }

  public GetAclForStoreGrpcResponse getAclForStore(GetAclForStoreGrpcRequest request) {
    ControllerRequestParamValidator.validateClusterStoreInfo(request.getStoreInfo());
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    LOGGER.info("Getting ACL for store: {} in cluster: {}", storeInfo.getStoreName(), storeInfo.getClusterName());
    String accessPermissions = admin.getAclForStore(storeInfo.getClusterName(), storeInfo.getStoreName());
    GetAclForStoreGrpcResponse.Builder builder = GetAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo);
    if (accessPermissions != null) {
      builder.setAccessPermissions(accessPermissions);
    }
    return builder.build();
  }

  public DeleteAclForStoreGrpcResponse deleteAclForStore(DeleteAclForStoreGrpcRequest request) {
    ControllerRequestParamValidator.validateClusterStoreInfo(request.getStoreInfo());
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    LOGGER.info("Deleting ACL for store: {} in cluster: {}", storeInfo.getStoreName(), storeInfo.getClusterName());
    admin.deleteAclForStore(storeInfo.getClusterName(), storeInfo.getStoreName());
    return DeleteAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo).build();
  }
}
