package com.linkedin.venice.controller;

import static com.linkedin.venice.controllerapi.transport.GrpcRequestResponseConverter.getClusterStoreGrpcInfo;
import static com.linkedin.venice.controllerapi.transport.GrpcRequestResponseConverter.getControllerRequest;

import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAclForStoreRequest;
import com.linkedin.venice.protocols.CheckResourceCleanupForStoreCreationGrpcRequest;
import com.linkedin.venice.protocols.CheckResourceCleanupForStoreCreationGrpcResponse;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.UpdateAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a gRPC service implementation for the VeniceController public API.
 */
public class VeniceControllerGrpcServiceImpl extends VeniceControllerGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerGrpcServiceImpl.class);

  private final VeniceControllerRequestHandler requestHandler;

  public VeniceControllerGrpcServiceImpl(VeniceControllerRequestHandler requestHandler) {
    this.requestHandler = requestHandler;
  }

  @Override
  public void getLeaderController(
      LeaderControllerGrpcRequest request,
      StreamObserver<LeaderControllerGrpcResponse> responseObserver) {
    String clusterName = request.getClusterName();
    LOGGER.info("Received gRPC request to get leader controller for cluster: {}", clusterName);
    try {
      LeaderControllerResponse response = new LeaderControllerResponse();
      requestHandler.getLeaderController(request.getClusterName(), response);
      responseObserver.onNext(
          LeaderControllerGrpcResponse.newBuilder()
              .setClusterName(response.getCluster())
              .setHttpUrl(response.getUrl())
              .setHttpsUrl(response.getSecureUrl())
              .setGrpcUrl(response.getGrpcUrl())
              .setSecureGrpcUrl(response.getSecureGrpcUrl())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while getting leader controller for cluster: {}", request.getClusterName(), e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while getting leader controller")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void createStore(
      CreateStoreGrpcRequest grpcRequest,
      StreamObserver<CreateStoreGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.debug("Received gRPC request to create store: {} in cluster: {}", storeName, clusterName);
    try {
      // TODO (sushantmane) : Add the ACL check for allowlist users here

      // Convert the gRPC request to the internal request object
      NewStoreRequest request = new NewStoreRequest(
          grpcRequest.getClusterStoreInfo().getClusterName(),
          grpcRequest.getClusterStoreInfo().getStoreName(),
          grpcRequest.hasOwner() ? grpcRequest.getOwner() : null,
          grpcRequest.getKeySchema(),
          grpcRequest.getValueSchema(),
          grpcRequest.hasAccessPermission() ? grpcRequest.getAccessPermission() : null,
          grpcRequest.getIsSystemStore());

      // Create the store using the internal request object
      NewStoreResponse response = new NewStoreResponse();
      requestHandler.createStore(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          CreateStoreGrpcResponse.newBuilder()
              .setClusterStoreInfo(getClusterStoreGrpcInfo(response))
              .setOwner(response.getOwner())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      // Log the error with structured details
      LOGGER.error("Error while creating store: {} in cluster: {}", storeName, clusterName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while creating store: ")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void updateAclForStore(
      UpdateAclForStoreGrpcRequest grpcRequest,
      StreamObserver<UpdateAclForStoreGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.debug("Received gRPC request to update ACL for store: {} in cluster: {}", storeName, clusterName);
    try {
      // Convert the gRPC request to the internal request object
      UpdateAclForStoreRequest request = new UpdateAclForStoreRequest(
          grpcRequest.getClusterStoreInfo().getClusterName(),
          grpcRequest.getClusterStoreInfo().getStoreName(),
          grpcRequest.getAccessPermissions());

      // Create the store using the internal request object
      AclResponse response = new AclResponse();
      requestHandler.updateAclForStore(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          UpdateAclForStoreGrpcResponse.newBuilder().setClusterStoreInfo(getClusterStoreGrpcInfo(response)).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while updating ACL for store: {} in cluster: {}", storeName, clusterName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while updating ACL for store: ")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void getAclForStore(
      GetAclForStoreGrpcRequest grpcRequest,
      StreamObserver<GetAclForStoreGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.debug("Received gRPC request to get ACL for store: {} in cluster: {}", storeName, clusterName);
    try {
      // Convert the gRPC request to the internal request object
      ControllerRequest request = getControllerRequest(grpcRequest.getClusterStoreInfo());

      // Create the store using the internal request object
      AclResponse response = new AclResponse();
      requestHandler.getAclForStore(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          GetAclForStoreGrpcResponse.newBuilder()
              .setClusterStoreInfo(getClusterStoreGrpcInfo(response))
              .setAccessPermissions(response.getAccessPermissions())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while getting ACL for store: {} in cluster: {}", storeName, clusterName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while getting ACL for store: ")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void deleteAclForStore(
      DeleteAclForStoreGrpcRequest grpcRequest,
      StreamObserver<DeleteAclForStoreGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.debug("Received gRPC request to delete ACL for store: {} in cluster: {}", storeName, clusterName);
    try {
      // Convert the gRPC request to the internal request object
      ControllerRequest request = getControllerRequest(grpcRequest.getClusterStoreInfo());

      // Create the store using the internal request object
      AclResponse response = new AclResponse();
      requestHandler.deleteAclForStore(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          DeleteAclForStoreGrpcResponse.newBuilder().setClusterStoreInfo(getClusterStoreGrpcInfo(response)).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while deleting ACL for store: {} in cluster: {}", storeName, clusterName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while deleting ACL for store: ")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void checkResourceCleanupForStoreCreation(
      CheckResourceCleanupForStoreCreationGrpcRequest grpcRequest,
      StreamObserver<CheckResourceCleanupForStoreCreationGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.debug(
        "Received gRPC request to check resource cleanup before creating store: {} in cluster: {}",
        storeName,
        clusterName);
    try {
      // Convert the gRPC request to the internal request object
      ControllerRequest request = getControllerRequest(grpcRequest.getClusterStoreInfo());

      // Create the store using the internal request object
      ControllerResponse response = new ControllerResponse();
      requestHandler.checkResourceCleanupBeforeStoreCreation(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          CheckResourceCleanupForStoreCreationGrpcResponse.newBuilder()
              .setClusterStoreInfo(getClusterStoreGrpcInfo(response))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error(
          "Error while checking resource cleanup before creating store: {} in cluster: {}",
          storeName,
          clusterName,
          e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while checking resource cleanup before creating store: ")
              .withCause(e)
              .asRuntimeException());
    }
  }
}
