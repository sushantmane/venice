package com.linkedin.venice.grpc;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.request.RouterRequest;
import io.grpc.stub.StreamObserver;


/**
 * We need to keep track of each request as it goes through the pipeline so that we can record the necessary metrics
 * and separate different parts of the logic for the response. If a specific handler raises an error, we set
 * the hasError flag to true and stop executing the rest of the pipeline excluding the stats collection.
 */
public class GrpcRequestContext<T> {
  private StreamObserver<T> responseObserver;

  private boolean isCompleted = false;
  private boolean hasError = false;
  private RouterRequest routerRequest;
  private ReadResponse readResponse;
  private ServerStatsContext serverStatsContext;
  boolean isOldApi = true;

  public GrpcRequestContext(StreamObserver<T> responseObserver) {
    this.responseObserver = responseObserver;
    // this.veniceServerResponseBuilder.setErrorCode(VeniceReadResponseStatus.OK.getCode());
  }

  public void setGrpcStatsContext(ServerStatsContext serverStatsContext) {
    this.serverStatsContext = serverStatsContext;
  }

  public ServerStatsContext getGrpcStatsContext() {
    return serverStatsContext;
  }

  public StreamObserver<T> getResponseObserver() {
    return responseObserver;
  }

  public void setResponseObserver(StreamObserver<T> responseObserver) {
    this.responseObserver = responseObserver;
  }

  public RouterRequest getRouterRequest() {
    return routerRequest;
  }

  public void setRouterRequest(RouterRequest routerRequest) {
    this.routerRequest = routerRequest;
  }

  public ReadResponse getReadResponse() {
    return readResponse;
  }

  public void setReadResponse(ReadResponse readResponse) {
    this.readResponse = readResponse;
  }

  public void setCompleted() {
    isCompleted = true;
  }

  public boolean isCompleted() {
    return isCompleted;
  }

  public boolean hasError() {
    return hasError;
  }

  public void setError() {
    hasError = true;
  }

  boolean isOldApi() {
    return isOldApi;
  }

  public void setOldApi(boolean isOldApi) {
    this.isOldApi = isOldApi;
  }
}
