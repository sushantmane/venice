package com.linkedin.venice.pubsub.api;

import java.util.Arrays;
import java.util.Objects;


/**
 * A key-value pair that is associated with a message
 */
public class PubsubMessageHeader {
  private final String key;
  private final byte[] value;

  public PubsubMessageHeader(String key, byte[] value) {
    this.key = Objects.requireNonNull(key, "PubsubMessage header key cannot be null");
    this.value = value;
  }

  public String key() {
    return key;
  }

  public byte[] value() {
    return value;
  }

  @Override
  public int hashCode() {
    return 31 * key.hashCode() + Arrays.hashCode(value);
  }

  @Override
  public boolean equals(Object otherObj) {
    if (this == otherObj) {
      return true;
    }
    if (!(otherObj instanceof PubsubMessageHeader)) {
      return false;
    }

    PubsubMessageHeader otherHeader = (PubsubMessageHeader) otherObj;
    return key.equals(otherHeader.key()) && Arrays.equals(value, otherHeader.value());
  }
}
