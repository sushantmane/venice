package com.linkedin.venice.vpj.pubsub.input;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.nio.ByteBuffer;


public final class UnifiedPubSubRecord {
  public enum Kind {
    PUT, DELETE
  }

  private final Kind kind;
  private final byte[] rawKeyBytes;
  private final byte[] chunkedKeySuffixBytes; // nullable
  private final ByteBuffer value; // empty for DELETE
  private final int schemaId;
  private final ByteBuffer replicationMetadataPayload;
  private final int replicationMetadataVersionId;
  private final long secondaryOffset; // numeric record offset or locally-built index
  private final PubSubPosition position; // exact PubSub position

  public UnifiedPubSubRecord(
      Kind kind,
      byte[] rawKeyBytes,
      byte[] chunkedKeySuffixBytes,
      ByteBuffer value,
      int schemaId,
      ByteBuffer replicationMetadataPayload,
      int replicationMetadataVersionId,
      long secondaryOffset,
      PubSubPosition position) {
    this.kind = kind;
    this.rawKeyBytes = rawKeyBytes;
    this.chunkedKeySuffixBytes = chunkedKeySuffixBytes;
    this.value = value;
    this.schemaId = schemaId;
    this.replicationMetadataPayload = replicationMetadataPayload;
    this.replicationMetadataVersionId = replicationMetadataVersionId;
    this.secondaryOffset = secondaryOffset;
    this.position = position;
  }

  public Kind getKind() {
    return kind;
  }

  public byte[] getRawKeyBytes() {
    return rawKeyBytes;
  }

  public byte[] getChunkedKeySuffixBytes() {
    return chunkedKeySuffixBytes;
  }

  public ByteBuffer getValue() {
    return value;
  }

  public int getSchemaId() {
    return schemaId;
  }

  public ByteBuffer getReplicationMetadataPayload() {
    return replicationMetadataPayload;
  }

  public int getReplicationMetadataVersionId() {
    return replicationMetadataVersionId;
  }

  public long getSecondaryOffset() {
    return secondaryOffset;
  }

  public PubSubPosition getPosition() {
    return position;
  }
}
