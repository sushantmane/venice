package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import com.linkedin.venice.fastclient.GetRequestContext;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


public interface FastClientTransport {
  CompletableFuture<TransportClientResponse> singleGet(GetRequestContext getRequestContext, String route);

  default void streamPost(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody,
      TransportClientStreamingCallback callback,
      int keyCount) {
    throw new VeniceClientException("'streamPost' is not supported.");
  }

  default CompletableFuture<TransportClientResponse> get(String requestPath) {
    return get(requestPath, Collections.EMPTY_MAP);
  }

  default CompletableFuture<TransportClientResponse> post(String requestPath, byte[] requestBody) {
    return post(requestPath, Collections.EMPTY_MAP, requestBody);
  }

  CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers);

  CompletableFuture<TransportClientResponse> post(String requestPath, Map<String, String> headers, byte[] requestBody);

  /**
   * If the internal client could not be used by its callback function,
   * implementation of this function should return a new copy.
   * The default implementation is to return itself.
   * @return
   */
  default FastClientTransport getCopyIfNotUsableInCallback() {
    return this;
  }
}
