package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controller.grpc.ControllerGrpcConstants.GRPC_CONTROLLER_CLIENT_DETAILS;
import static com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils.handleRequest;
import static com.linkedin.venice.controller.server.VeniceRouteHandler.ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX;

import com.linkedin.venice.controller.grpc.server.GrpcControllerClientDetails;
import com.linkedin.venice.exceptions.VeniceUnauthorizedAccessException;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a gRPC service implementation for the VeniceController public API.
 */
public class VeniceControllerGrpcServiceImpl extends VeniceControllerGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerGrpcServiceImpl.class);

  private final VeniceControllerRequestHandler requestHandler;
  private final VeniceControllerAccessManager accessManager;

  public VeniceControllerGrpcServiceImpl(VeniceControllerRequestHandler requestHandler) {
    this.requestHandler = requestHandler;
    this.accessManager = requestHandler.getControllerAccessManager();
  }

  protected GrpcControllerClientDetails getClientDetails(Context context) {
    GrpcControllerClientDetails clientDetails = GRPC_CONTROLLER_CLIENT_DETAILS.get(context);
    if (clientDetails == null) {
      clientDetails = GrpcControllerClientDetails.UNDEFINED_CLIENT_DETAILS;
    }
    return clientDetails;
  }

  public boolean isAllowListUser(String resourceName, Context context) {
    GrpcControllerClientDetails clientDetails = getClientDetails(context);
    return accessManager.isAllowListUser(resourceName, clientDetails.getClientCertificate());
  }

  @Override
  public void getLeaderController(
      LeaderControllerGrpcRequest request,
      StreamObserver<LeaderControllerGrpcResponse> responseObserver) {
    LOGGER.debug("Received getLeaderController with args: {}", request);
    handleRequest(
        VeniceControllerGrpcServiceGrpc.getGetLeaderControllerMethod(),
        () -> requestHandler.getLeaderControllerDetails(request),
        responseObserver,
        request.getClusterName(),
        null);
  }

  @Override
  public void discoverClusterForStore(
      DiscoverClusterGrpcRequest grpcRequest,
      StreamObserver<DiscoverClusterGrpcResponse> responseObserver) {
    LOGGER.debug("Received discoverClusterForStore with args: {}", grpcRequest);
    handleRequest(
        VeniceControllerGrpcServiceGrpc.getDiscoverClusterForStoreMethod(),
        () -> requestHandler.discoverCluster(grpcRequest),
        responseObserver,
        null,
        grpcRequest.getStoreName());
  }

  @Override
  public void createStore(
      CreateStoreGrpcRequest grpcRequest,
      StreamObserver<CreateStoreGrpcResponse> responseObserver) {
    LOGGER.debug("Received createStore with args: {}", grpcRequest);
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    handleRequest(VeniceControllerGrpcServiceGrpc.getCreateStoreMethod(), () -> {
      if (!isAllowListUser(grpcRequest.getClusterStoreInfo().getStoreName(), Context.current())) {
        throw new VeniceUnauthorizedAccessException(
            ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX
                + VeniceControllerGrpcServiceGrpc.getCreateStoreMethod().getFullMethodName() + " on resource: "
                + storeName);
      }
      return requestHandler.createStore(grpcRequest);
    }, responseObserver, clusterName, storeName);
  }
}
