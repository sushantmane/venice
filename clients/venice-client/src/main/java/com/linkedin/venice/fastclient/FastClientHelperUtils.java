package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import java.nio.ByteBuffer;


public class FastClientHelperUtils {
  static ByteBuffer decompressRecord(
      CompressionStrategy compressionStrategy,
      VeniceCompressor compressor,
      ByteBuffer data,
      String storeName,
      int version) {
    try {
      if (compressor == null) {
        throw new VeniceClientException(
            String.format(
                "Expected to find compressor in metadata but found null, compressionStrategy:%s, store:%s, version:%d",
                compressionStrategy,
                storeName,
                version));
      }
      return compressor.decompress(data);
    } catch (Exception e) {
      throw new VeniceClientException(
          String.format(
              "Unable to decompress the record, compressionStrategy:%s store:%s version:%d",
              compressionStrategy,
              storeName,
              version),
          e);
    }
  }
}
