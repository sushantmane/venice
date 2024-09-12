package com.linkedin.venice.grpc;

import static com.linkedin.venice.listener.QuotaEnforcementHandler.QuotaEnforcementResult.ALLOWED;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.INVALID_REQUEST_RESOURCE_MSG;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.SERVER_OVER_CAPACITY_MSG;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.QuotaEnforcementHandler;
import com.linkedin.venice.listener.QuotaEnforcementHandler.QuotaEnforcementResult;
import com.linkedin.venice.listener.RequestStatsRecorder;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.protocols.MultiKeyResponse;
import com.linkedin.venice.protocols.SingleGetResponse;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GrpcIoRequestProcessor {
  private static final Logger LOGGER = LogManager.getLogger(GrpcIoRequestProcessor.class);
  private final QuotaEnforcementHandler quotaEnforcementHandler;
  private final StorageReadRequestHandler storageReadRequestHandler;

  public GrpcIoRequestProcessor(GrpcServiceDependencies services) {
    this.quotaEnforcementHandler = services.getQuotaEnforcementHandler();
    this.storageReadRequestHandler = services.getStorageReadRequestHandler();
  }

  public void processRequest(GrpcRequestContext requestContext) {
    RouterRequest request = requestContext.getRouterRequest();

    QuotaEnforcementResult result = quotaEnforcementHandler.enforceQuota(request);
    // If the request is allowed, hand it off to the storage read request handler
    if (result == ALLOWED) {
      GrpcStorageResponseHandlerCallback callback = GrpcStorageResponseHandlerCallback.create(requestContext);
      storageReadRequestHandler.queueIoRequestForAsyncProcessing(request, callback);
      return;
    }

    // Otherwise, set an error response based on the quota enforcement result
    switch (result) {
      case BAD_REQUEST:
        requestContext.setErrorMessage(INVALID_REQUEST_RESOURCE_MSG + request.getResourceName());
        requestContext.setReadResponseStatus(VeniceReadResponseStatus.BAD_REQUEST);
        break;
      case REJECTED:
        requestContext.setReadResponseStatus(VeniceReadResponseStatus.TOO_MANY_REQUESTS);
        requestContext.setErrorMessage("Quota exceeded for resource: " + request.getResourceName());
        break;
      case OVER_CAPACITY:
        requestContext.setReadResponseStatus(VeniceReadResponseStatus.SERVICE_UNAVAILABLE);
        requestContext.setErrorMessage(SERVER_OVER_CAPACITY_MSG);
        break;
      default:
        requestContext.setReadResponseStatus(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR);
        requestContext.setErrorMessage("Unknown quota enforcement result: " + result);
    }
    sendResponse(requestContext);
  }

  public static <T> void sendResponse(GrpcRequestContext<T> requestContext) {
    GrpcRequestContext.GrpcRequestType grpcRequestType = requestContext.getGrpcRequestType();
    switch (grpcRequestType) {
      case SINGLE_GET:
        sendSingleGetResponse((GrpcRequestContext<SingleGetResponse>) requestContext);
        break;
      case MULTI_GET:
      case COMPUTE:
        sendMultiKeyResponse((GrpcRequestContext<MultiKeyResponse>) requestContext);
        break;
      case LEGACY:
        sendVeniceServerResponse((GrpcRequestContext<VeniceServerResponse>) requestContext);
        break;
      default:
        VeniceException veniceException = new VeniceException("Unknown response type: " + grpcRequestType);
        LOGGER.error("Unknown response type: {}", grpcRequestType, veniceException);
        throw veniceException;
    }
  }

  public static void sendSingleGetResponse(GrpcRequestContext<SingleGetResponse> requestContext) {
    ReadResponse readResponse = requestContext.getReadResponse();
    SingleGetResponse.Builder builder = SingleGetResponse.newBuilder();
    VeniceReadResponseStatus responseStatus = requestContext.getReadResponseStatus();
    RequestStatsRecorder requestStatsRecorder = requestContext.getRequestStatsRecorder();
    requestStatsRecorder.setResponseStatus(responseStatus);

    if (readResponse == null) {
      builder.setStatusCode(requestContext.getReadResponseStatus().getCode());
      builder.setErrorMessage(requestContext.getErrorMessage());
      requestStatsRecorder.setResponseStatus(requestContext.getReadResponseStatus());
    } else if (readResponse.isFound()) {
      builder.setRcu(readResponse.getRCU())
          .setStatusCode(responseStatus.getCode())
          .setSchemaId(readResponse.getResponseSchemaIdHeader())
          .setCompressionStrategy(readResponse.getCompressionStrategy().getValue())
          .setContentLength(readResponse.getResponseBody().readableBytes())
          .setContentType(HttpConstants.AVRO_BINARY)
          .setValue(GrpcUtils.toByteString(readResponse.getResponseBody()));
    } else {
      builder.setStatusCode(responseStatus.getCode())
          .setRcu(readResponse.getRCU())
          .setErrorMessage("Key not found")
          .setContentLength(0);
    }

    StreamObserver<SingleGetResponse> responseObserver = requestContext.getResponseObserver();
    synchronized (responseObserver) {
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }

    reportRequestStats(requestContext);
  }

  public static void sendMultiKeyResponse(GrpcRequestContext<MultiKeyResponse> requestContext) {
    ReadResponse readResponse = requestContext.getReadResponse();
    MultiKeyResponse.Builder builder = MultiKeyResponse.newBuilder();

    VeniceReadResponseStatus responseStatus = requestContext.getReadResponseStatus();
    RequestStatsRecorder requestStatsRecorder = requestContext.getRequestStatsRecorder();
    requestStatsRecorder.setResponseStatus(responseStatus);

    if (readResponse == null) {
      builder.setStatusCode(responseStatus.getCode());
      builder.setErrorMessage(requestContext.getErrorMessage());
    } else if (readResponse.isFound()) {
      builder.setStatusCode(responseStatus.getCode())
          .setRcu(readResponse.getRCU())
          .setCompressionStrategy(readResponse.getCompressionStrategy().getValue())
          .setContentLength(readResponse.getResponseBody().readableBytes())
          .setContentType(HttpConstants.AVRO_BINARY)
          .setValue(GrpcUtils.toByteString(readResponse.getResponseBody()));
    } else {
      builder.setStatusCode(responseStatus.getCode())
          .setRcu(readResponse.getRCU())
          .setErrorMessage("Key not found")
          .setContentLength(0);
    }

    StreamObserver<MultiKeyResponse> responseObserver = requestContext.getResponseObserver();
    synchronized (responseObserver) {
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }
    reportRequestStats(requestContext);
  }

  public static void sendVeniceServerResponse(GrpcRequestContext<VeniceServerResponse> requestContext) {
    ReadResponse readResponse = requestContext.getReadResponse();
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();

    VeniceReadResponseStatus responseStatus = requestContext.getReadResponseStatus();
    RequestStatsRecorder requestStatsRecorder = requestContext.getRequestStatsRecorder();
    requestStatsRecorder.setResponseStatus(responseStatus);

    if (readResponse == null) {
      builder.setErrorCode(responseStatus.getCode());
      builder.setErrorMessage(requestContext.getErrorMessage());
    } else if (readResponse.isFound()) {
      builder.setErrorCode(responseStatus.getCode())
          .setResponseRCU(readResponse.getRCU())
          .setCompressionStrategy(readResponse.getCompressionStrategy().getValue())
          .setIsStreamingResponse(readResponse.isStreamingResponse())
          .setSchemaId(readResponse.getResponseSchemaIdHeader())
          .setData(GrpcUtils.toByteString(readResponse.getResponseBody()));
    } else {
      builder.setErrorCode(responseStatus.getCode()).setErrorMessage("Key not found").setData(ByteString.EMPTY);
    }

    StreamObserver<VeniceServerResponse> responseObserver = requestContext.getResponseObserver();
    synchronized (responseObserver) {
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }

    reportRequestStats(requestContext);
  }

  public static void reportRequestStats(GrpcRequestContext requestContext) {
    RequestStatsRecorder.recordRequestCompletionStats(requestContext.getRequestStatsRecorder(), true, -1);
  }
}
