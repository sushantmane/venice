package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A callback interface to handle the response from the transport layer. This is per route callback.
 * @param <K>
 * @param <V>
 */
public class TransportRouteResponseHandlerCallback<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(DispatchingAvroGenericStoreClient.class);
  private final CompletableFuture<Void> routeRequestFuture = new CompletableFuture<>();

  private MultiKeyRequestContext<K, V> requestContext;
  private StreamingCallback<K, V> streamingCallback;
  private List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes;
  private String routeId;

  public TransportRouteResponseHandlerCallback(
      String route,
      MultiKeyRequestContext<K, V> requestContext,
      StreamingCallback<K, V> streamingCallback,
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes) {
    this.routeId = route + requestContext.computeRequestUri();
  }

  public CompletableFuture<Void> getRouteRequestFuture() {
    return routeRequestFuture;
  }

  public void onRecordReceived(MultiGetResponseRecordV1 record) {
    LOGGER.info("Received record: {}", record);
  }

  public void onRecordReceived(List<MultiGetResponseRecordV1> records) {
    LOGGER.info("Received records: {}", records);
  }

  public void onCompleted() {
    LOGGER.info("Completed processing records");
  }

  public void onError(Throwable t) {
    LOGGER.error("Error processing records", t);
  }
}
