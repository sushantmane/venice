package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAclForStoreRequest;
import com.linkedin.venice.controllerapi.transport.GrpcRequestResponseConverter;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.QueryJobStatusGrpcRequest;
import com.linkedin.venice.protocols.QueryJobStatusGrpcResponse;
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

      // Convert the internal response object to the gRPC response object
      CreateStoreGrpcResponse.Builder grpcResponseBuilder = CreateStoreGrpcResponse.newBuilder();
      grpcResponseBuilder.setClusterStoreInfo(GrpcRequestResponseConverter.getClusterStoreGrpcInfo(response));
      grpcResponseBuilder.setOwner(response.getOwner());

      // Send the gRPC response
      responseObserver.onNext(grpcResponseBuilder.build());
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

      // Convert the internal response object to the gRPC response object
      UpdateAclForStoreGrpcResponse.Builder grpcResponseBuilder = UpdateAclForStoreGrpcResponse.newBuilder();
      grpcResponseBuilder.setClusterStoreInfo(GrpcRequestResponseConverter.getClusterStoreGrpcInfo(response));

      // Send the gRPC response
      responseObserver.onNext(grpcResponseBuilder.build());
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
  public void getLeaderController(
      LeaderControllerGrpcRequest request,
      StreamObserver<LeaderControllerGrpcResponse> responseObserver) {
    String clusterName = request.getClusterName();
    LOGGER.info("Received gRPC request to get leader controller for cluster: {}", clusterName);
    LeaderControllerGrpcResponse.Builder responseBuilder = LeaderControllerGrpcResponse.newBuilder();
    try {
      LeaderControllerResponse leaderControllerResponse = new LeaderControllerResponse();
      requestHandler.getLeaderController(request.getClusterName(), leaderControllerResponse);
      responseBuilder.setClusterName(leaderControllerResponse.getCluster());
      responseBuilder.setHttpUrl(leaderControllerResponse.getUrl());
      responseBuilder.setHttpsUrl(leaderControllerResponse.getSecureUrl());
      responseBuilder.setGrpcUrl(leaderControllerResponse.getGrpcUrl());
      responseBuilder.setSecureGrpcUrl(leaderControllerResponse.getSecureGrpcUrl());
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while getting leader controller for cluster: {}", request.getClusterName(), e);
    }
  }

  @Override
  public void queryJobStatus(
      QueryJobStatusGrpcRequest request,
      StreamObserver<QueryJobStatusGrpcResponse> responseObserver) {
  }
}
