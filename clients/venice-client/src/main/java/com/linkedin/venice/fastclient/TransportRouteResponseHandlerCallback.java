package com.linkedin.venice.fastclient;

import static com.linkedin.venice.fastclient.DispatchingAvroGenericStoreClient.FC_REDUNDANT_LOGGING_FILTER;

import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.LatencyUtils;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
  private DispatchingAvroGenericStoreClient<K, V> dispatchingAvroGenericStoreClient;
  private long totalDecompressionTimeForResponse = 0;
  private Set<Integer> keysSeen;

  public TransportRouteResponseHandlerCallback(
      String route,
      MultiKeyRequestContext<K, V> requestContext,
      StreamingCallback<K, V> streamingCallback,
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes,
      DispatchingAvroGenericStoreClient<K, V> dispatchingAvroGenericStoreClient) {
    this.routeId = route;
    this.requestContext = requestContext;
    this.streamingCallback = streamingCallback;
    this.keysForRoutes = keysForRoutes;
    this.dispatchingAvroGenericStoreClient = dispatchingAvroGenericStoreClient;
    this.keysSeen = new HashSet<>(keysForRoutes.size());
  }

  public CompletableFuture<Void> getRouteRequestFuture() {
    return routeRequestFuture;
  }

  public void onRecordReceived(
      int keyIndex,
      int schemaId,
      ByteBuffer compressedValue,
      CompressionStrategy compressionStrategy) {
    VeniceCompressor compressor = dispatchingAvroGenericStoreClient.getMetadata()
        .getCompressor(compressionStrategy, requestContext.getCurrentVersion());

    // Step 1: Decompress the record
    long nanoTsBeforeDecompression = System.nanoTime();
    ByteBuffer decompressRecord = FastClientHelperUtils
        .decompressRecord(compressionStrategy, compressor, compressedValue, requestContext.getResourceName());

    // Step 2: Deserialize the record
    long nanoTsBeforeDeserialization = System.nanoTime();
    totalDecompressionTimeForResponse += nanoTsBeforeDeserialization - nanoTsBeforeDecompression;
    RecordDeserializer<V> dataRecordDeserializer =
        dispatchingAvroGenericStoreClient.getDataRecordDeserializer(schemaId);
    V deserializedValue = dataRecordDeserializer.deserialize(decompressRecord);
    requestContext.recordRecordDeserializationTime(routeId, LatencyUtils.getLatencyInNS(nanoTsBeforeDeserialization));

    // Step 3: Call the streaming callback
    keysSeen.add(keyIndex);
    MultiKeyRequestContext.KeyInfo<K> keyInfo = keysForRoutes.get(keyIndex);
    streamingCallback.onRecordReceived(keyInfo.getKey(), deserializedValue);
  }

  public void onRecordReceived(List<MultiGetResponseRecordV1> records) {
    // call streamingCallback.onRecordReceived(key, value); for each record
  }

  public void onCompleted() {
    // Record the decompression time for the route
    requestContext.recordDecompressionTime(routeId, totalDecompressionTimeForResponse);

    // call streamingCallback.onRecordReceived(key, null); for each key not seen
    for (int i = 0; i < keysForRoutes.size(); i++) {
      if (!keysSeen.contains(i)) {
        streamingCallback.onRecordReceived(keysForRoutes.get(i).getKey(), null);
      }
    }

    // complete route specific future
    routeRequestFuture.complete(null);
  }

  public void onError(Throwable t) {
    if (!FC_REDUNDANT_LOGGING_FILTER
        .isRedundantException(dispatchingAvroGenericStoreClient.getBatchGetTransportExceptionFilterMessage())) {
      LOGGER.error("Exception received from transport.", t);
    }
    requestContext.markCompleteExceptionally(t);
    routeRequestFuture.completeExceptionally(t);
  }
}
