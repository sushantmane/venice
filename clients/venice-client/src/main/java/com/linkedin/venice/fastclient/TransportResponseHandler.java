package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TransportResponseHandler<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(DispatchingAvroGenericStoreClient.class);

  private final MultiKeyRequestContext<K, V> requestContext;
  private final StreamingCallback<K, V> userCallback;
  private final String storeName;
  private final DispatchingAvroGenericStoreClient<K, V> client;
  private final RecordDeserializer<MultiGetResponseRecordV1> responseDeserializer;
  private final VeniceCompressor compressor;
  private final List<MultiKeyRequestContext.KeyInfo<K>> keyInfos;
  private final Set<Integer> keysSeen = new HashSet<>();

  public TransportResponseHandler(
      MultiKeyRequestContext<K, V> requestContext,
      StreamingCallback<K, V> userCallback,
      String storeName,
      DispatchingAvroGenericStoreClient<K, V> client) {
    this.requestContext = requestContext;
    this.userCallback = userCallback;
    this.storeName = storeName;
    this.client = client;
    this.responseDeserializer = client.getMultiGetResponseRecordDeserializer(transportClientResponse.getSchemaId());
    this.compressor = client.getMetadata()
        .getCompressor(transportClientResponse.getCompressionStrategy(), requestContext.currentVersion);
    this.keyInfos = requestContext.keysForRoutes(transportClientResponse.getRouteId());
  }

  public void handleResponse(Throwable exception) {
    if (exception != null) {
      onError(exception);
      return;
    }

    try {
      long startDeserialization = System.nanoTime();
      Iterable<MultiGetResponseRecordV1> records = responseDeserializer
          .deserializeObjects(new ByteBufferOptimizedBinaryDecoder(transportClientResponse.getBody()));
      requestContext.recordRequestDeserializationTime(
          transportClientResponse.getRouteId(),
          LatencyUtils.getLatencyInNS(startDeserialization));

      long totalDecompressionTime = 0;
      List<V> batchRecords = new ArrayList<>();

      for (MultiGetResponseRecordV1 record: records) {
        long decompressionStart = System.nanoTime();
        V value = processRecord(record);
        totalDecompressionTime += System.nanoTime() - decompressionStart;
        userCallback.onRecord(keyInfos.get(record.keyIndex).getKey(), value);
        batchRecords.add(value);
      }

      userCallback.onRecords(batchRecords);
      requestContext.recordDecompressionTime(transportClientResponse.getRouteId(), totalDecompressionTime);
      onCompletion();
    } catch (Exception e) {
      onError(e);
    }
  }

  private V processRecord(MultiGetResponseRecordV1 record) throws Exception {
    int keyIndex = record.keyIndex;
    ByteBuffer decompressedValue = FastClientHelperUtils.decompressRecord(
        transportClientResponse.getCompressionStrategy(),
        compressor,
        record.value,
        storeName,
        requestContext.currentVersion);

    RecordDeserializer<V> valueDeserializer = client.getDataRecordDeserializer(record.getSchemaId());
    V value = valueDeserializer.deserialize(decompressedValue);
    requestContext.recordRecordDeserializationTime(
        transportClientResponse.getRouteId(),
        LatencyUtils.getLatencyInNS(System.nanoTime()));

    keysSeen.add(keyIndex);
    return value;
  }

  private void onCompletion() {
    // Handle missing keys by sending null for any key not seen
    for (int i = 0; i < keyInfos.size(); i++) {
      if (!keysSeen.contains(i)) {
        userCallback.onRecord(keyInfos.get(i).getKey(), null);
      }
    }
    userCallback.onCompletion();
    requestContext.markComplete(transportClientResponse);
    transportClientResponse.getRouteRequestFuture().complete(SC_OK);
  }

  private void onError(Throwable e) {
    requestContext.markCompleteExceptionally(transportClientResponse, e);
    transportClientResponse.getRouteRequestFuture().completeExceptionally(e);
  }
}
