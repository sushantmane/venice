package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.FastClientHelperUtils;
import com.linkedin.venice.fastclient.GetRequestContext;
import com.linkedin.venice.fastclient.MultiKeyRequestContext;
import com.linkedin.venice.fastclient.TransportRouteResponseHandlerCallback;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.LatencyUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.avro.io.ByteBufferOptimizedBinaryDecoder;


/**
 * Adapter class to adapt {@link TransportClient} impl to {@link FastClientTransport}.
 */
public class R2FastClientTransportAdapter implements FastClientTransport {
  private TransportClient transportClient;

  public R2FastClientTransportAdapter(TransportClient transportClient) {
    this.transportClient = transportClient;
  }

  @Override
  public CompletableFuture<TransportClientResponse> singleGet(String route, GetRequestContext getRequestContext) {
    return transportClient.get(route + getRequestContext.computeRequestUri());
  }

  @Override
  public <K, V> void multiKeyStreamingRequest(
      String route,
      MultiKeyRequestContext<K, V> requestContext,
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes,
      Function<List<MultiKeyRequestContext.KeyInfo<K>>, byte[]> requestSerializer,
      Map<String, String> requestHeaders,
      TransportRouteResponseHandlerCallback<K, V> routeResponseHandlerCallback) {
    // serialize the request
    String url = route + requestContext.computeRequestUri();
    long nanoTsBeforeSerialization = System.nanoTime();
    byte[] serializedRequest = requestSerializer.apply(keysForRoutes);
    requestContext.recordRequestSerializationTime(route, LatencyUtils.getLatencyInNS(nanoTsBeforeSerialization));

    // send the request
    CompletableFuture<TransportClientResponse> responseFuture =
        transportClient.post(url, requestHeaders, serializedRequest);

    // handle the response
    responseFuture.whenComplete((transportClientResponse, throwable) -> {
      if (throwable != null) {
        routeResponseHandlerCallback.onError(throwable);
        return;
      }

      int responseRecordWrapperSchemaId = transportClientResponse.getSchemaId();
      byte[] body = transportClientResponse.getBody();
      CompressionStrategy compressionStrategy = transportClientResponse.getCompressionStrategy();

      // deserialize records and find the status
      RecordDeserializer<MultiGetResponseRecordV1> deserializer =
          FastClientHelperUtils.getMultiGetResponseRecordDeserializer(responseRecordWrapperSchemaId);

      long nanoTsBeforeRequestDeserialization = System.nanoTime();
      Iterable<MultiGetResponseRecordV1> records =
          deserializer.deserializeObjects(new ByteBufferOptimizedBinaryDecoder(body));
      requestContext
          .recordRequestDeserializationTime(route, LatencyUtils.getLatencyInNS(nanoTsBeforeRequestDeserialization));

      // process the records
      for (MultiGetResponseRecordV1 recordV1: records) {
        int keyIndex = recordV1.getKeyIndex();
        int schemaId = recordV1.getSchemaId();
        ByteBuffer value = recordV1.getValue();
        routeResponseHandlerCallback.onRecordReceived(keyIndex, schemaId, value, compressionStrategy);
      }

      // mark the route as complete
      routeResponseHandlerCallback.onCompleted();
    });
  }

  @Override
  public void close() throws IOException {
    transportClient.close();
  }
}
