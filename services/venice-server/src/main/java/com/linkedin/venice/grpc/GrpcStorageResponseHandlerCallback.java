package com.linkedin.venice.grpc;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.listener.StorageResponseHandlerCallback;
import com.linkedin.venice.response.VeniceReadResponseStatus;


/**
 * This class is a callback that is used to handle the response from the storage layer.
 *
 * Except for the cases where the response was returned before the storage layer was invoked, this class is used to
 * handle the response from the storage layer and pass it to the next handler in the pipeline.
 */
public class GrpcStorageResponseHandlerCallback implements StorageResponseHandlerCallback {
  private final GrpcRequestContext requestContext;

  private GrpcStorageResponseHandlerCallback(GrpcRequestContext requestContext) {
    this.requestContext = requestContext;
  }

  // Factory method for creating an instance of this class.
  public static GrpcStorageResponseHandlerCallback create(GrpcRequestContext requestContext) {
    return new GrpcStorageResponseHandlerCallback(requestContext);
  }

  @Override
  public void onReadResponse(ReadResponse readResponse) {
    requestContext.setReadResponse(readResponse);
    GrpcIoRequestProcessor.sendResponse(requestContext);
  }

  @Override
  public void onError(VeniceReadResponseStatus readResponseStatus, String message) {
    requestContext.setError();
    requestContext.setReadResponseStatus(readResponseStatus);
    requestContext.setErrorMessage(message);
    requestContext.setReadResponse(null);
    GrpcIoRequestProcessor.sendResponse(requestContext);
  }
}
