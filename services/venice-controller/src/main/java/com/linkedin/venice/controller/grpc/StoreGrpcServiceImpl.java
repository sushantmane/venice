package com.linkedin.venice.controller.grpc;

import com.linkedin.venice.controller.server.StoreRequestHandler;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreGrpcServiceImpl extends StoreGrpcServiceGrpc.StoreGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(StoreGrpcServiceImpl.class);
  private final StoreRequestHandler requestHandler;

  public StoreGrpcServiceImpl(StoreRequestHandler requestHandler) {
    this.requestHandler = requestHandler;
  }

  @Override
  public void updateAclForStore(
      UpdateAclForStoreGrpcRequest request,
      StreamObserver<UpdateAclForStoreGrpcResponse> responseObserver) {
    UpdateAclForStoreGrpcResponse response = requestHandler.updateAclForStore(request);
  }

  @Override
  public void getAclForStore(
      GetAclForStoreGrpcRequest request,
      StreamObserver<GetAclForStoreGrpcResponse> responseObserver) {
    GetAclForStoreGrpcResponse response = requestHandler.getAclForStore(request);
  }

  @Override
  public void deleteAclForStore(
      DeleteAclForStoreGrpcRequest request,
      StreamObserver<DeleteAclForStoreGrpcResponse> responseObserver) {
    DeleteAclForStoreGrpcResponse response = requestHandler.deleteAclForStore(request);
  }
}
