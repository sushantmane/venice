package com.linkedin.venice.grpc;

import static com.linkedin.venice.grpc.GrpcRequestContext.GrpcRequestType.COMPUTE;
import static com.linkedin.venice.grpc.GrpcRequestContext.GrpcRequestType.LEGACY;
import static com.linkedin.venice.grpc.GrpcRequestContext.GrpcRequestType.MULTI_GET;
import static com.linkedin.venice.grpc.GrpcRequestContext.GrpcRequestType.SINGLE_GET;
import static com.linkedin.venice.listener.StorageReadRequestHandler.VENICE_STORAGE_NODE_HARDWARE_IS_NOT_HEALTHY_MSG;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.request.ComputeRouterRequestWrapper;
import com.linkedin.venice.listener.request.CurrentVersionRequest;
import com.linkedin.venice.listener.request.DictionaryFetchRequest;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MetadataFetchRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.request.TopicPartitionIngestionContextRequest;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.protocols.AdminRequest;
import com.linkedin.venice.protocols.AdminResponse;
import com.linkedin.venice.protocols.ComputeRequest;
import com.linkedin.venice.protocols.CurrentVersionInfoRequest;
import com.linkedin.venice.protocols.CurrentVersionInfoResponse;
import com.linkedin.venice.protocols.GetCompressionDictionaryRequest;
import com.linkedin.venice.protocols.GetCompressionDictionaryResponse;
import com.linkedin.venice.protocols.HealthCheckRequest;
import com.linkedin.venice.protocols.HealthCheckResponse;
import com.linkedin.venice.protocols.IngestionContextRequest;
import com.linkedin.venice.protocols.IngestionContextResponse;
import com.linkedin.venice.protocols.MetadataRequest;
import com.linkedin.venice.protocols.MetadataResponse;
import com.linkedin.venice.protocols.MultiGetRequest;
import com.linkedin.venice.protocols.MultiKeyResponse;
import com.linkedin.venice.protocols.SingleGetRequest;
import com.linkedin.venice.protocols.SingleGetResponse;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.utils.ObjectMapperFactory;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class implements the Venice Read Service gRPC service.
 */
public class VeniceGrpcReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceGrpcReadServiceImpl.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private final DiskHealthCheckService diskHealthCheckService;
  private final StorageReadRequestHandler storageReadRequestHandler;
  private final GrpcServiceDependencies dependencies;
  private final GrpcIoRequestProcessor requestProcessor;

  public VeniceGrpcReadServiceImpl(GrpcServiceDependencies dependencies) {
    this(dependencies, new GrpcIoRequestProcessor(dependencies));
  }

  VeniceGrpcReadServiceImpl(GrpcServiceDependencies dependencies, GrpcIoRequestProcessor requestProcessor) {
    this.dependencies = dependencies;
    this.requestProcessor = requestProcessor;
    this.diskHealthCheckService = dependencies.getDiskHealthCheckService();
    this.storageReadRequestHandler = dependencies.getStorageReadRequestHandler();
  }

  /**
   * @deprecated This method is deprecated and will be removed in the future. Use the {@link #singleGet(SingleGetRequest, StreamObserver)} method instead.
   */
  @Deprecated
  @Override
  public void get(VeniceClientRequest singleGetRequest, StreamObserver<VeniceServerResponse> streamObserver) {
    GrpcRequestContext<VeniceServerResponse> clientRequestCtx =
        GrpcRequestContext.create(dependencies, streamObserver, LEGACY);
    try {
      RouterRequest routerRequest = GetRouterRequest.parseSingleGetGrpcRequest(singleGetRequest);
      clientRequestCtx.getRequestStatsRecorder()
          .setRequestInfo(routerRequest)
          .setRequestSize(singleGetRequest.getSerializedSize());
      clientRequestCtx.setRouterRequest(routerRequest);
      requestProcessor.processRequest(clientRequestCtx);
    } catch (Exception e) {
      LOGGER.debug("Error while processing single get request", e);
      VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
      builder.setErrorCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  /**
   * @deprecated This method is deprecated and will be removed in the future. Use the {@link #multiGet(MultiGetRequest, StreamObserver)} method instead.
   */
  @Deprecated
  @Override
  public void batchGet(VeniceClientRequest batchGetRequest, StreamObserver<VeniceServerResponse> streamObserver) {
    GrpcRequestContext<VeniceServerResponse> requestContext =
        GrpcRequestContext.create(dependencies, streamObserver, LEGACY);
    try {
      RouterRequest routerRequest = MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest(batchGetRequest);
      requestContext.getRequestStatsRecorder()
          .setRequestInfo(routerRequest)
          .setRequestSize(batchGetRequest.getSerializedSize());
      requestContext.setRouterRequest(routerRequest);
      requestProcessor.processRequest(requestContext);
    } catch (Exception e) {
      LOGGER.debug("Error while processing batch-get request", e);
      VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
      builder.setErrorCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  @Override
  public void singleGet(SingleGetRequest singleGetRequest, StreamObserver<SingleGetResponse> streamObserver) {
    GrpcRequestContext<SingleGetResponse> requestContext =
        GrpcRequestContext.create(dependencies, streamObserver, SINGLE_GET);
    try {
      RouterRequest routerRequest = GetRouterRequest.parseSingleGetGrpcRequest(singleGetRequest);
      requestContext.getRequestStatsRecorder()
          .setRequestInfo(routerRequest)
          .setRequestSize(singleGetRequest.getSerializedSize());
      requestContext.setRouterRequest(routerRequest);
      requestProcessor.processRequest(requestContext);
    } catch (Exception e) {
      LOGGER.debug("Error while processing single get request", e);
      SingleGetResponse.Builder builder = SingleGetResponse.newBuilder();
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  @Override
  public void multiGet(MultiGetRequest multiGetRequest, StreamObserver<MultiKeyResponse> streamObserver) {
    GrpcRequestContext<MultiKeyResponse> requestContext =
        GrpcRequestContext.create(dependencies, streamObserver, MULTI_GET);
    try {
      RouterRequest routerRequest = MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest(multiGetRequest);
      requestContext.getRequestStatsRecorder()
          .setRequestInfo(routerRequest)
          .setRequestSize(multiGetRequest.getSerializedSize());
      requestContext.setRouterRequest(routerRequest);
      requestProcessor.processRequest(requestContext);
    } catch (Exception e) {
      LOGGER.debug("Error while processing multi get request", e);
      MultiKeyResponse.Builder builder = MultiKeyResponse.newBuilder();
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  @Override
  public void compute(ComputeRequest computeRequest, StreamObserver<MultiKeyResponse> responseObserver) {
    GrpcRequestContext<MultiKeyResponse> requestContext =
        GrpcRequestContext.create(dependencies, responseObserver, COMPUTE);
    try {
      RouterRequest routerRequest = ComputeRouterRequestWrapper.parseComputeGrpcRequest(computeRequest);
      requestContext.getRequestStatsRecorder()
          .setRequestInfo(routerRequest)
          .setRequestSize(computeRequest.getSerializedSize());
      requestContext.setRouterRequest(routerRequest);
      requestProcessor.processRequest(requestContext);
    } catch (Exception e) {
      LOGGER.debug("Error while processing compute request", e);
      MultiKeyResponse.Builder builder = MultiKeyResponse.newBuilder();
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void isServerHealthy(
      HealthCheckRequest healthCheckRequest,
      StreamObserver<HealthCheckResponse> responseObserver) {
    HealthCheckResponse.Builder builder = HealthCheckResponse.newBuilder();
    if (diskHealthCheckService.isDiskHealthy()) {
      builder.setStatusCode(VeniceReadResponseStatus.OK.getCode());
    } else {
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      builder.setMessage(VENICE_STORAGE_NODE_HARDWARE_IS_NOT_HEALTHY_MSG);
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getCompressionDictionary(
      GetCompressionDictionaryRequest dictionaryRequest,
      StreamObserver<GetCompressionDictionaryResponse> responseObserver) {
    GetCompressionDictionaryResponse.Builder builder = GetCompressionDictionaryResponse.newBuilder();
    try {
      DictionaryFetchRequest dictionaryFetchRequest = DictionaryFetchRequest.parseGetGrpcRequest(dictionaryRequest);
      BinaryResponse binaryResponse = storageReadRequestHandler.handleDictionaryFetchRequest(dictionaryFetchRequest);
      builder.setStatusCode(VeniceReadResponseStatus.OK.getCode());
      builder.setValue(GrpcUtils.toByteString(binaryResponse.getBody()));
    } catch (Exception e) {
      LOGGER.error("Error while processing dictionary request", e);
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage("Error while processing dictionary request: " + e.getMessage());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void handleAdminRequest(AdminRequest request, StreamObserver<AdminResponse> responseObserver) {
    AdminResponse.Builder builder = AdminResponse.newBuilder();
    try {
      com.linkedin.davinci.listener.response.AdminResponse adminResponse = storageReadRequestHandler
          .handleServerAdminRequest(com.linkedin.venice.listener.request.AdminRequest.parseAdminGrpcRequest(request));
      if (!adminResponse.isError()) {
        builder.setSchemaId(com.linkedin.davinci.listener.response.AdminResponse.getResponseSchemaIdHeader());
        builder.setStatusCode(VeniceReadResponseStatus.OK.getCode());
        builder.setValue(GrpcUtils.toByteString(adminResponse.getResponseBody()));
        builder.setContentType(HttpConstants.AVRO_BINARY);
        builder.setContentLength(adminResponse.getResponseBody().readableBytes());
      } else {
        builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
        String errorMessage = adminResponse.getMessage() != null ? adminResponse.getMessage() : "Unknown error";
        builder.setErrorMessage(errorMessage);
        builder.setContentType(HttpConstants.TEXT_PLAIN);
      }
    } catch (Exception e) {
      LOGGER.error("Error while processing admin request", e);
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage("Error while processing admin request: " + e.getMessage());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getMetadata(MetadataRequest request, StreamObserver<MetadataResponse> responseObserver) {
    MetadataResponse.Builder builder = MetadataResponse.newBuilder();
    try {
      MetadataFetchRequest metadataFetchRequest = MetadataFetchRequest.parseGetGrpcRequest(request);
      com.linkedin.davinci.listener.response.MetadataResponse metadataResponse =
          storageReadRequestHandler.handleMetadataFetchRequest(metadataFetchRequest);
      if (!metadataResponse.isError()) {
        builder.setStatusCode(VeniceReadResponseStatus.OK.getCode());
        builder.setValue(GrpcUtils.toByteString(metadataResponse.getResponseBody()));
        builder.setContentType(HttpConstants.AVRO_BINARY);
        builder.setContentLength(metadataResponse.getResponseBody().readableBytes());
        builder.setSchemaId(metadataResponse.getResponseSchemaIdHeader());
      } else {
        String errorMessage = metadataResponse.getMessage() != null ? metadataResponse.getMessage() : "Unknown error";
        builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
        builder.setErrorMessage(errorMessage);
        builder.setContentType(HttpConstants.TEXT_PLAIN);
      }
    } catch (Exception e) {
      LOGGER.error("Error while processing metadata request", e);
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage("Error while processing metadata request: " + e.getMessage());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getCurrentVersionInfo(
      CurrentVersionInfoRequest request,
      StreamObserver<CurrentVersionInfoResponse> responseObserver) {
    CurrentVersionInfoResponse.Builder builder = CurrentVersionInfoResponse.newBuilder();
    try {
      CurrentVersionRequest currentVersionRequest = CurrentVersionRequest.parseGetGrpcRequest(request);
      ServerCurrentVersionResponse currentVersionResponse =
          storageReadRequestHandler.handleCurrentVersionRequest(currentVersionRequest);
      if (!currentVersionResponse.isError()) {
        builder.setStatusCode(VeniceReadResponseStatus.OK.getCode());
        builder.setCurrentVersion(currentVersionResponse.getCurrentVersion());
      } else {
        String errorMessage =
            currentVersionResponse.getMessage() != null ? currentVersionResponse.getMessage() : "Unknown error";
        builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
        builder.setErrorMessage(errorMessage);
      }
    } catch (Exception e) {
      LOGGER.error("Error while processing current version info request", e);
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage("Error while processing current version info request: " + e.getMessage());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getIngestionContext(
      IngestionContextRequest request,
      StreamObserver<IngestionContextResponse> responseObserver) {
    IngestionContextResponse.Builder builder = IngestionContextResponse.newBuilder();
    try {
      TopicPartitionIngestionContextRequest ingestionContextRequest =
          TopicPartitionIngestionContextRequest.parseGetGrpcRequest(request);
      TopicPartitionIngestionContextResponse response =
          storageReadRequestHandler.handleTopicPartitionIngestionContextRequest(ingestionContextRequest);
      if (!response.isError()) {
        builder.setStatusCode(VeniceReadResponseStatus.OK.getCode());
        ByteBuf body = Unpooled.wrappedBuffer(OBJECT_MAPPER.writeValueAsBytes(response));
        builder.setValue(GrpcUtils.toByteString(body));
        builder.setContentType(HttpConstants.JSON);
        builder.setContentLength(body.readableBytes());
      } else {
        String errorMessage = response.getMessage() != null ? response.getMessage() : "Unknown error";
        builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
        builder.setErrorMessage(errorMessage);
        builder.setContentType(HttpConstants.TEXT_PLAIN);
      }
    } catch (Exception e) {
      LOGGER.error("Error while processing ingestion context request", e);
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage("Error while processing ingestion context request: " + e.getMessage());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
