package com.linkedin.venice.grpc;

import com.linkedin.venice.listener.StorageReadRequestHandler;


public class GrpcStorageReadRequestHandler extends VeniceServerGrpcHandler {
  private final StorageReadRequestHandler readRequestHandler;

  public GrpcStorageReadRequestHandler(StorageReadRequestHandler readRequestHandler) {
    this.readRequestHandler = readRequestHandler;
  }

  @Override
  public void processRequest(GrpcRequestContext ctx) {
    GrpcStorageResponseHandlerCallback callback = GrpcStorageResponseHandlerCallback.create(ctx, this);
    readRequestHandler.processRequest(ctx.getRouterRequest(), callback);
  }
}
