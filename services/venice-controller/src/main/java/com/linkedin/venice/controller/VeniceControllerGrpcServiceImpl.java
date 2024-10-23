package com.linkedin.venice.controller;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.transport.GrpcRequestResponseConverter;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.QueryJobStatusGrpcRequest;
import com.linkedin.venice.protocols.QueryJobStatusGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase;
import io.grpc.protobuf.StatusProto;
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
      // Convert the gRPC request to the internal request object
      NewStoreRequest request = GrpcRequestResponseConverter.convertGrpcRequestToNewStoreRequest(grpcRequest);

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

      // Build an error info message with details
      ErrorInfo errorInfo = ErrorInfo.newBuilder()
          .setReason("STORE_CREATION_FAILED")
          .putMetadata("storeName", storeName)
          .putMetadata("clusterName", clusterName)
          .putMetadata("errorMessage", e.getMessage())
          .build();

      // Build the gRPC status with the error details
      com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
          .setCode(Code.INTERNAL.getNumber())
          .setMessage("Failed to create store: " + storeName + " in cluster: " + clusterName)
          .addDetails(Any.pack(errorInfo))
          .build();

      // Return the error response with structured details
      responseObserver.onError(StatusProto.toStatusRuntimeException(status));
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
      // Build an error info message with details
      ErrorInfo errorInfo = ErrorInfo.newBuilder()
          .setReason("LEADER_DISCOVER_FAILED")
          .putMetadata("clusterName", clusterName)
          .putMetadata("errorMessage", e.getMessage())
          .build();

      // Build the gRPC status with the error details
      com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
          .setCode(Code.INTERNAL.getNumber())
          .setMessage("Failed to get leader controller for cluster: " + clusterName)
          .addDetails(Any.pack(errorInfo))
          .build();

      // Return the error response with structured details
      responseObserver.onError(StatusProto.toStatusRuntimeException(status));
    }
  }

  @Override
  public void queryJobStatus(
      QueryJobStatusGrpcRequest request,
      StreamObserver<QueryJobStatusGrpcResponse> responseObserver) {
  }
}
