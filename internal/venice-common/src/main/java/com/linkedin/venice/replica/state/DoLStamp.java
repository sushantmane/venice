package com.linkedin.venice.replica.state;

import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import java.nio.ByteBuffer;


/**
 * Declaration of Leadership Stamp (DoLStamp) is a record sent by the leader to let followers know
 * that it is taking over the leadership of the partition.
 */
public class DoLStamp {
  public static final String DOL_STAMP_TERM_ID = "DoLStamp";
  public static final String DOL_STAMP_TIMESTAMP = "DoLTs";
  public static final long DOL_SENTINEL_OFFSET = -1L;
  private final long termId;
  private final long timestamp;
  private long offset = DOL_SENTINEL_OFFSET;

  // TODO(sushant): Add token to DoLStamp if offset + termId is not enough to uniquely identify if DoLStamp is new or
  // old in the last step of the processing pipeline (persistence layer).
  // private String dolToken;

  public DoLStamp(long termId, long timestamp) {
    this.termId = termId;
    this.timestamp = timestamp;
  }

  public DoLStamp(byte[] termIdBytes, byte[] timestampBytes) {
    this.termId = getLongFromBytes(termIdBytes);
    this.timestamp = getLongFromBytes(timestampBytes);
  }

  public long getTermId() {
    return termId;
  }

  public byte[] getTimestampBytes() {
    return getBytesFromLong(timestamp);
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public PubSubMessageHeader getTermIdHeader() {
    return new PubSubMessageHeader(DOL_STAMP_TERM_ID, getTermIdBytes());
  }

  public PubSubMessageHeader getTimestampHeader() {
    return new PubSubMessageHeader(DOL_STAMP_TIMESTAMP, getTimestampBytes());
  }

  public PubSubMessageHeaders getHeaders() {
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(getTermIdHeader());
    headers.add(getTimestampHeader());
    return headers;
  }

  public byte[] getTermIdBytes() {
    return getBytesFromLong(termId);
  }

  // convert byte array to term id
  public static long getLongFromBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.put(bytes);
    buffer.flip();// need flip
    return buffer.getLong();
  }

  public static byte[] getBytesFromLong(long termId) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(termId);
    return buffer.array();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DoLStamp doLStamp = (DoLStamp) o;

    if (termId != doLStamp.termId) {
      return false;
    }
    if (timestamp != doLStamp.timestamp) {
      return false;
    }
    return offset == doLStamp.offset;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(termId);
    result = 31 * result + Long.hashCode(timestamp);
    result = 31 * result + Long.hashCode(offset);
    return result;
  }

  @Override
  public String toString() {
    return "DoLStamp{" + "termId=" + termId + ", ts=" + timestamp + ", offset=" + offset + '}';
  }
}
