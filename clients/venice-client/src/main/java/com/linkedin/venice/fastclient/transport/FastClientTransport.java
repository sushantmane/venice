package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.fastclient.GetRequestContext;
import com.linkedin.venice.fastclient.MultiKeyRequestContext;
import com.linkedin.venice.fastclient.TransportRouteResponseHandlerCallback;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


public interface FastClientTransport {
  CompletableFuture<TransportClientResponse> singleGet(String route, GetRequestContext getRequestContext);

  <K, V> void multiKeyStreamingRequest(
      String route,
      MultiKeyRequestContext<K, V> requestContext,
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes,
      Function<List<MultiKeyRequestContext.KeyInfo<K>>, byte[]> requestSerializer,
      Map<String, String> headers,
      TransportRouteResponseHandlerCallback<K, V> callback);

  void close() throws IOException;
}
