package com.linkedin.venice.grpc;

import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.INVALID_REQUEST_RESOURCE_MSG;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.QuotaEnforcementResult.ALLOWED;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.QuotaEnforcementResult.BAD_REQUEST;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.QuotaEnforcementResult.OVER_CAPACITY;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.QuotaEnforcementResult.REJECTED;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.SERVER_OVER_CAPACITY_MSG;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.ReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.ReadQuotaEnforcementHandler.QuotaEnforcementResult;
import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.protocols.MultiGetResponse;
import com.linkedin.venice.protocols.SingleGetResponse;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.utils.LatencyUtils;
import io.grpc.stub.StreamObserver;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GrpcIoRequestProcessor {
  private static final Logger LOGGER = LogManager.getLogger(GrpcIoRequestProcessor.class);
  private final ReadQuotaEnforcementHandler readQuotaEnforcementHandler;
  private final StorageReadRequestHandler storageReadRequestHandler;

  public GrpcIoRequestProcessor(GrpcServiceDependencies services) {
    this.readQuotaEnforcementHandler = services.getReadQuotaEnforcementHandler();
    this.storageReadRequestHandler = services.getStorageReadRequestHandler();
  }

  public void processRequest(GrpcRequestContext requestContext) {
    RouterRequest request = requestContext.getRouterRequest();
    QuotaEnforcementResult result =
        readQuotaEnforcementHandler != null ? readQuotaEnforcementHandler.enforceQuota(request) : null;
    // If the request is allowed, hand it off to the storage read request handler
    if (result == null || result == ALLOWED) {
      GrpcStorageResponseHandlerCallback callback = GrpcStorageResponseHandlerCallback.create(requestContext);
      storageReadRequestHandler.processIoRequestAsync(request, callback);
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
    requestContext.setError();
    sendResponse(requestContext);
  }

  public static <T> void sendResponse(GrpcRequestContext<T> requestContext) {
    Class<?> responseType = requestContext.getResponseType();

    if (responseType == SingleGetResponse.class) {
      sendSingleGetResponse((GrpcRequestContext<SingleGetResponse>) requestContext);
    } else if (responseType == MultiGetResponse.class) {
      sendMultiGetResponse((GrpcRequestContext<MultiGetResponse>) requestContext);
    } else if (responseType == VeniceServerResponse.class) {
      sendVeniceServerResponse((GrpcRequestContext<VeniceServerResponse>) requestContext);
    } else {
      // log stack trace and throw exception
      VeniceException veniceException = new VeniceException("Unknown response type: " + responseType);
      LOGGER.error("Unknown response type: {}", responseType, veniceException);
      throw veniceException;
    }
  }

  public static void sendSingleGetResponse(GrpcRequestContext<SingleGetResponse> requestContext) {
    ReadResponse readResponse = requestContext.getReadResponse();
    SingleGetResponse.Builder builder = SingleGetResponse.newBuilder()
        .setRcu(readResponse.getRCU())
        .setSchemaId(readResponse.getResponseSchemaIdHeader())
        .setCompressionStrategy(readResponse.getCompressionStrategy().getValue());
    if (readResponse == null) {
      builder.setStatusCode(requestContext.getReadResponseStatus().getCode());
      builder.setErrorMessage(requestContext.getErrorMessage());
    } else if (readResponse.isFound()) {
      builder.setContentLength(readResponse.getResponseBody().readableBytes());
      builder.setContentType(HttpConstants.AVRO_BINARY);
      builder.setValue(GrpcUtils.toByteString(readResponse.getResponseBody()));
      requestContext.getStatsContext().setResponseStatus(OK);
    } else {
      requestContext.setError();
      requestContext.getStatsContext().setResponseStatus(NOT_FOUND);
      builder.setStatusCode(VeniceReadResponseStatus.KEY_NOT_FOUND.getCode());
      builder.setErrorMessage("Key not found");
      builder.setContentLength(0);
    }
    StreamObserver<SingleGetResponse> responseObserver = requestContext.getResponseObserver();

    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();

    reportRequestStats(requestContext);
  }

  public static void sendMultiGetResponse(GrpcRequestContext<MultiGetResponse> requestContext) {
    ReadResponse readResponse = requestContext.getReadResponse();
    MultiGetResponse.Builder builder = MultiGetResponse.newBuilder();
    builder.setRcu(readResponse.getRCU());
    builder.setCompressionStrategy(readResponse.getCompressionStrategy().getValue());
    if (readResponse == null) {
      builder.setStatusCode(requestContext.getReadResponseStatus().getCode());
      builder.setErrorMessage(requestContext.getErrorMessage());
    } else if (readResponse.isFound()) {
      builder.setContentLength(readResponse.getResponseBody().readableBytes());
      builder.setContentType(HttpConstants.AVRO_BINARY);
      builder.setValue(GrpcUtils.toByteString(readResponse.getResponseBody()));
      requestContext.getStatsContext().setResponseStatus(OK);
    } else {
      requestContext.setError();
      requestContext.getStatsContext().setResponseStatus(NOT_FOUND);
      builder.setStatusCode(VeniceReadResponseStatus.KEY_NOT_FOUND.getCode());
      builder.setErrorMessage("Key not found");
      builder.setContentLength(0);
    }

    StreamObserver<MultiGetResponse> responseObserver = requestContext.getResponseObserver();
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();

    reportRequestStats(requestContext);
  }

  public static void sendVeniceServerResponse(GrpcRequestContext<VeniceServerResponse> requestContext) {
    ReadResponse readResponse = requestContext.getReadResponse();
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    builder.setResponseRCU(readResponse.getRCU());
    builder.setCompressionStrategy(readResponse.getCompressionStrategy().getValue());
    builder.setIsStreamingResponse(readResponse.isStreamingResponse());
    if (readResponse == null) {
      builder.setErrorCode(requestContext.getReadResponseStatus().getCode());
      builder.setErrorMessage(requestContext.getErrorMessage());
    } else if (!readResponse.isFound()) {
      requestContext.setError();
      requestContext.getStatsContext().setResponseStatus(NOT_FOUND);
      builder.setErrorCode(VeniceReadResponseStatus.KEY_NOT_FOUND.getCode());
      builder.setErrorMessage("Key not found");
      builder.setData(ByteString.EMPTY);
    } else {
      builder.setData(GrpcUtils.toByteString(readResponse.getResponseBody()));
      builder.setSchemaId(readResponse.getResponseSchemaIdHeader());
      requestContext.getStatsContext().setResponseStatus(OK);
    }

    StreamObserver<VeniceServerResponse> responseObserver = requestContext.getResponseObserver();
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();

    reportRequestStats(requestContext);
  }

  public static void reportRequestStats(GrpcRequestContext requestContext) {
    ServerStatsContext statsContext = requestContext.getStatsContext();
    HttpResponseStatus responseStatus = statsContext.getResponseStatus();
    if (statsContext.getResponseStatus() == null) {
      LOGGER.error("Received error in outbound gRPC Stats Handler: response status could not be null");
      return;
    }

    String storeName = statsContext.getStoreName();
    ServerHttpRequestStats serverHttpRequestStats;

    if (statsContext.getStoreName() == null) {
      LOGGER.error("Received error in outbound gRPC Stats Handler: store name could not be null");
      return;
    } else {
      serverHttpRequestStats = statsContext.getCurrentStats().getStoreStats(storeName);
      statsContext.recordBasicMetrics(serverHttpRequestStats);
    }

    double elapsedTime = LatencyUtils.getElapsedTimeFromNSToMS(statsContext.getRequestStartTimeInNS());
    if (!requestContext.hasError() && !responseStatus.equals(OK) || responseStatus.equals(NOT_FOUND)) {
      statsContext.successRequest(serverHttpRequestStats, elapsedTime);
    } else {
      statsContext.errorRequest(serverHttpRequestStats, elapsedTime);
    }
  }
}
