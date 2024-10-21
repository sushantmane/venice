package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.nio.ByteBuffer;


public class FastClientHelperUtils {
  public static ByteBuffer decompressRecord(
      CompressionStrategy compressionStrategy,
      VeniceCompressor compressor,
      ByteBuffer data,
      String storeVersionName) {
    try {
      if (compressor != null) {
        return compressor.decompress(data);
      }
      throw new VeniceClientException(
          String.format(
              "Expected to find compressor in metadata but found null, compressionStrategy:%s, store-version:%s",
              compressionStrategy,
              storeVersionName));
    } catch (Exception e) {
      throw new VeniceClientException(
          String.format(
              "Unable to decompress the record, compressionStrategy:%s store-version:%s",
              compressionStrategy,
              storeVersionName),
          e);
    }
  }

  /* Batch get helper methods */
  public static RecordDeserializer<MultiGetResponseRecordV1> getMultiGetResponseRecordDeserializer(int schemaId) {
    // TODO: get multi-get response write schema from Router

    return FastSerializerDeserializerFactory
        .getFastAvroSpecificDeserializer(MultiGetResponseRecordV1.SCHEMA$, MultiGetResponseRecordV1.class);
  }
}
