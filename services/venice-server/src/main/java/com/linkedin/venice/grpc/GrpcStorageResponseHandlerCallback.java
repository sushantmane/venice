package com.linkedin.venice.grpc;

import static com.linkedin.venice.response.VeniceReadResponseStatus.MISROUTED_STORE_VERSION;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.listener.RequestStatsRecorder;
import com.linkedin.venice.listener.StorageResponseHandlerCallback;
import com.linkedin.venice.listener.VeniceRequestEarlyTerminationException;
import com.linkedin.venice.listener.response.AbstractReadResponse;
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
    // TODO: move this stats reporting to {@link GrpcIoRequestProcessor#reportRequestStats}
    RequestStatsRecorder statsRecorder = requestContext.getRequestStatsRecorder();
    AbstractReadResponse abstractReadResponse = (AbstractReadResponse) readResponse;
    if (readResponse.isFound()) {
      requestContext.setReadResponseStatus(VeniceReadResponseStatus.OK);
      statsRecorder.setResponseStatus(VeniceReadResponseStatus.OK)
          .setReadResponseStats(abstractReadResponse.getStatsRecorder())
          .setResponseSize(abstractReadResponse.getResponseBody().readableBytes());
    } else {
      requestContext.setReadResponseStatus(VeniceReadResponseStatus.KEY_NOT_FOUND);
      statsRecorder.setResponseStatus(VeniceReadResponseStatus.KEY_NOT_FOUND)
          .setReadResponseStats(abstractReadResponse.getStatsRecorder())
          .setResponseSize(0);
    }

    requestContext.setReadResponse(readResponse);
    GrpcIoRequestProcessor.sendResponse(requestContext);
  }

  @Override
  public void onError(VeniceReadResponseStatus readResponseStatus, String message) {
    // TODO: move this stats reporting to {@link GrpcIoRequestProcessor#reportRequestStats}
    RequestStatsRecorder statsRecorder = requestContext.getRequestStatsRecorder().setResponseStatus(readResponseStatus);

    if (readResponseStatus == VeniceRequestEarlyTerminationException.getResponseStatusCode()) {
      statsRecorder.setRequestTerminatedEarly();
    }
    if (readResponseStatus == MISROUTED_STORE_VERSION) {
      statsRecorder.setMisroutedStoreVersion(true);
    }

    requestContext.setError();
    requestContext.setReadResponseStatus(readResponseStatus);
    requestContext.setErrorMessage(message);
    requestContext.setReadResponse(null);
    GrpcIoRequestProcessor.sendResponse(requestContext);
  }
}
