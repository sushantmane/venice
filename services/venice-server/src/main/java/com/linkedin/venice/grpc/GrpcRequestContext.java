package com.linkedin.venice.grpc;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.listener.RequestStatsRecorder;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.grpc.stub.StreamObserver;


/**
 * This class is used to store the context of a gRPC request.
 * @param <T> the type of the response observer
 */
public class GrpcRequestContext<T> {
  private final StreamObserver<T> responseObserver;
  private final GrpcRequestType grpcRequestType;
  private final RequestStatsRecorder requestStatsRecorder;
  private RouterRequest routerRequest;
  private ReadResponse readResponse = null;
  private VeniceReadResponseStatus readResponseStatus = null;
  private String errorMessage;

  enum GrpcRequestType {
    LEGACY, SINGLE_GET, MULTI_GET, COMPUTE
  }

  private GrpcRequestContext(
      RequestStatsRecorder requestStatsRecorder,
      StreamObserver<T> responseObserver,
      GrpcRequestType grpcRequestType) {
    this.requestStatsRecorder = requestStatsRecorder;
    this.responseObserver = responseObserver;
    this.grpcRequestType = grpcRequestType;
  }

  public static <T> GrpcRequestContext<T> create(
      GrpcServiceDependencies services,
      StreamObserver<T> responseObserver,
      GrpcRequestType grpcRequestType) {
    return new GrpcRequestContext<>(
        new RequestStatsRecorder(services.getSingleGetStats(), services.getMultiGetStats(), services.getComputeStats()),
        responseObserver,
        grpcRequestType);
  }

  public RequestStatsRecorder getRequestStatsRecorder() {
    return requestStatsRecorder;
  }

  public StreamObserver<T> getResponseObserver() {
    return responseObserver;
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

  public VeniceReadResponseStatus getReadResponseStatus() {
    // If the readResponseStatus is set, return it.
    if (readResponseStatus != null) {
      return readResponseStatus;
    }

    // If the readResponse is set, return the appropriate status based on the response.
    if (readResponse != null && readResponse.isFound()) {
      return VeniceReadResponseStatus.OK;
    }

    // If the readResponse is set and the key is not found, return the appropriate status.
    if (readResponse != null && !readResponse.isFound()) {
      return VeniceReadResponseStatus.KEY_NOT_FOUND;
    }

    // If the readResponse is not set, return an internal server error.
    return VeniceReadResponseStatus.INTERNAL_SERVER_ERROR;
  }

  public void setReadResponseStatus(VeniceReadResponseStatus readResponseStatus) {
    this.readResponseStatus = readResponseStatus;
    if (requestStatsRecorder != null) {
      requestStatsRecorder.setResponseStatus(readResponseStatus);
    }
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public GrpcRequestType getGrpcRequestType() {
    return grpcRequestType;
  }
}
