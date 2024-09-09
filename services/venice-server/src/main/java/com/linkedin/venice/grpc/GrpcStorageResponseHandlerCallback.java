package com.linkedin.venice.grpc;

import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.venice.listener.StorageResponseHandlerCallback;
import com.linkedin.venice.listener.response.BinaryResponse;
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
    VeniceServerResponse.Builder responseBuilder = requestContext.getVeniceServerResponseBuilder();
    responseBuilder.setResponseRCU(readResponse.getRCU());
    // TODO: Figure out why this is needed.
    if (requestContext.getRouterRequest().isStreamingRequest()) {
      responseBuilder.setIsStreamingResponse(true);
    }
    requestContext.setReadResponse(readResponse);
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
    requestContext.getVeniceServerResponseBuilder().setErrorCode(readResponseStatus.getCode()).setErrorMessage(message);
    readRequestHandler.invokeNextHandler(requestContext);
  }
}
