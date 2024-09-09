package com.linkedin.venice.grpc;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.listener.StorageResponseHandlerCallback;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.listener.response.SingleGetResponseWrapper;
import com.linkedin.venice.protocols.MultiGetResponse;
import com.linkedin.venice.protocols.SingleGetResponse;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;


/**
 * This class is a callback that is used to handle the response from the storage layer.
 *
 * Except for the cases where the response was returned before the storage layer was invoked, this class is used to
 * handle the response from the storage layer and pass it to the next handler in the pipeline.
 */
public class GrpcStorageResponseHandlerCallback implements StorageResponseHandlerCallback {
  private final GrpcRequestContext requestContext;
  private GrpcStorageReadRequestHandler readRequestHandler;

  private GrpcStorageResponseHandlerCallback(
      GrpcRequestContext requestContext,
      GrpcStorageReadRequestHandler readRequestHandler) {
    this.requestContext = requestContext;
    this.readRequestHandler = readRequestHandler;
  }

  // Factory method for creating an instance of this class.
  public static GrpcStorageResponseHandlerCallback create(
      GrpcRequestContext requestContext,
      GrpcStorageReadRequestHandler readRequestHandler) {
    return new GrpcStorageResponseHandlerCallback(requestContext, readRequestHandler);
  }

  @Override
  public void onReadResponse(ReadResponse readResponse) {
    if (requestContext.isOldApi()) {
      VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
      builder.setResponseRCU(readResponse.getRCU());
      builder.setCompressionStrategy(readResponse.getCompressionStrategy().getValue());
      builder.setIsStreamingResponse(readResponse.isStreamingResponse());
      if (!readResponse.isFound()) {
        requestContext.setError();
        requestContext.getGrpcStatsContext().setResponseStatus(NOT_FOUND);
        builder.setErrorCode(VeniceReadResponseStatus.KEY_NOT_FOUND.getCode());
        builder.setErrorMessage("Key not found");
        builder.setData(ByteString.EMPTY);
      } else {
        builder.setData(GrpcUtils.toByteString(readResponse.getResponseBody()));
        builder.setSchemaId(readResponse.getResponseSchemaIdHeader());
        requestContext.getGrpcStatsContext().setResponseStatus(OK);
      }

      readRequestHandler.invokeNextHandler(requestContext);
      return;
    }

    if (readResponse instanceof SingleGetResponseWrapper) {
      SingleGetResponseWrapper singleGetResponseWrapper = (SingleGetResponseWrapper) readResponse;
      SingleGetResponse.Builder builder = SingleGetResponse.newBuilder();
      builder.setRcu(singleGetResponseWrapper.getRCU());
      builder.setSchemaId(singleGetResponseWrapper.getResponseSchemaIdHeader());
      builder.setCompressionStrategy(singleGetResponseWrapper.getCompressionStrategy().getValue());
      if (!singleGetResponseWrapper.isFound()) {
        requestContext.setError();
        requestContext.getGrpcStatsContext().setResponseStatus(NOT_FOUND);
        builder.setStatusCode(VeniceReadResponseStatus.KEY_NOT_FOUND.getCode());
        builder.setErrorMessage("Key not found");
        builder.setContentLength(0);
      } else {
        builder.setContentLength(singleGetResponseWrapper.getResponseBody().readableBytes());
        builder.setContentType(HttpConstants.AVRO_BINARY);
        builder.setValue(GrpcUtils.toByteString(singleGetResponseWrapper.getResponseBody()));
        requestContext.getGrpcStatsContext().setResponseStatus(OK);
      }
      readRequestHandler.invokeNextHandler(requestContext);
      return;
    }

    MultiGetResponse.Builder multiGetResponseBuilder = MultiGetResponse.newBuilder();
    multiGetResponseBuilder.setRcu(readResponse.getRCU());
    multiGetResponseBuilder.setCompressionStrategy(readResponse.getCompressionStrategy().getValue());
    if (!readResponse.isFound()) {
      requestContext.setError();
      requestContext.getGrpcStatsContext().setResponseStatus(NOT_FOUND);
      multiGetResponseBuilder.setStatusCode(VeniceReadResponseStatus.KEY_NOT_FOUND.getCode());
      multiGetResponseBuilder.setErrorMessage("Key not found");
      multiGetResponseBuilder.setContentLength(0);
    } else {
      multiGetResponseBuilder.setContentLength(readResponse.getResponseBody().readableBytes());
      multiGetResponseBuilder.setContentType(HttpConstants.AVRO_BINARY);
      multiGetResponseBuilder.setValue(GrpcUtils.toByteString(readResponse.getResponseBody()));
      requestContext.getGrpcStatsContext().setResponseStatus(OK);
    }
    readRequestHandler.invokeNextHandler(requestContext);
  }

  @Override
  public void onBinaryResponse(BinaryResponse binaryResponse) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void onAdminResponse(AdminResponse adminResponse) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void onMetadataResponse(MetadataResponse metadataResponse) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void onServerCurrentVersionResponse(ServerCurrentVersionResponse serverCurrentVersionResponse) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void onTopicPartitionIngestionContextResponse(
      TopicPartitionIngestionContextResponse topicPartitionIngestionContextResponse) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void onResponse(VeniceReadResponseStatus readResponseStatus, String message) {

  }

  @Override
  public void onError(VeniceReadResponseStatus readResponseStatus, String message) {
    requestContext.setError();
    requestContext.responseBuilder().setErrorCode(readResponseStatus.getCode()).setErrorMessage(message);
    readRequestHandler.invokeNextHandler(requestContext);
  }
}
