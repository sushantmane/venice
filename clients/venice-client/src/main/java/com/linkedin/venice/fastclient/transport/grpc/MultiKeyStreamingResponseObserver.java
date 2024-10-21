package com.linkedin.venice.fastclient.transport.grpc;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.TransportRouteResponseHandlerCallback;
import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.protocols.MultiKeyStreamingResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MultiKeyStreamingResponseObserver<K, V> implements StreamObserver<MultiKeyStreamingResponse> {
  private static final Logger LOGGER = LogManager.getLogger(MultiKeyStreamingResponseObserver.class);

  private final TransportRouteResponseHandlerCallback<K, V> routeResponseHandlerCallback;

  public MultiKeyStreamingResponseObserver(TransportRouteResponseHandlerCallback<K, V> routeResponseHandlerCallback) {
    this.routeResponseHandlerCallback = routeResponseHandlerCallback;
  }

  @Override
  public void onNext(MultiKeyStreamingResponse response) {
    try {
      VeniceReadResponseStatus readResponseStatus = VeniceReadResponseStatus.fromCode(response.getStatusCode());
      switch (readResponseStatus) {
        case OK:
          int keyIndex = response.getKeyIndex();
          int schemaId = response.getSchemaId();
          ByteBuffer value = GrpcUtils.toByteBuffer(response.getValue());
          CompressionStrategy compressionStrategy = CompressionStrategy.valueOf(response.getCompressionStrategy());
          routeResponseHandlerCallback.onRecordReceived(keyIndex, schemaId, value, compressionStrategy);
          break;
        case KEY_NOT_FOUND:
          completeWithResponse(null);
          break;
        case BAD_REQUEST:
          completeWithException(new VeniceClientHttpException(response.getErrorMessage(), response.getStatusCode()));
          break;
        case TOO_MANY_REQUESTS:
          completeWithException(new VeniceClientRateExceededException(response.getErrorMessage()));
          break;
        default:
          handleUnexpectedError(response);
          break;
      }
    } catch (IllegalArgumentException e) {
      handleUnknownStatusCode(response, e);
    }
    routeResponseHandlerCallback.onRecordReceived(value);
  }

  @Override
  public void onError(Throwable t) {
    // maybe convert to VeniceClientException?
    LOGGER.error("Error in multi-key streaming response with gRPC client", t);
    routeResponseHandlerCallback.onError(t);
  }

  @Override
  public void onCompleted() {
    routeResponseHandlerCallback.onCompleted();
  }
}
