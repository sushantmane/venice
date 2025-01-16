package com.linkedin.venice.controller.grpc;

import com.linkedin.venice.controller.server.VeniceControllerRequestHandler;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.StoreAclGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import io.grpc.stub.StreamObserver;


public class StoreAclGrpcServiceImpl extends StoreAclGrpcServiceGrpc.StoreAclGrpcServiceImplBase {
  private final VeniceControllerRequestHandler requestHandler;

  public StoreAclGrpcServiceImpl(VeniceControllerRequestHandler requestHandler) {
    this.requestHandler = requestHandler;
  }

  @Override
  public void updateAclForStore(
      UpdateAclForStoreGrpcRequest request,
      StreamObserver<com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse> responseObserver) {
    requestHandler.updateAclForStore(request, responseObserver);
  }

  @Override
  public void getAclForStore(
      GetAclForStoreGrpcRequest request,
      StreamObserver<GetAclForStoreGrpcResponse> responseObserver) {
    requestHandler.getAclForStore(request, responseObserver);
  }

  @Override
  public void deleteAclForStore(
      DeleteAclForStoreGrpcRequest request,
      StreamObserver<DeleteAclForStoreGrpcResponse> responseObserver) {
    requestHandler.deleteAclForStore(request, responseObserver);
  }
}
