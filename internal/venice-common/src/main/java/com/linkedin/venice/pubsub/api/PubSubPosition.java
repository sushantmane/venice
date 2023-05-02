package com.linkedin.venice.pubsub.api;

public interface PubSubPosition {
  int comparePosition(PubSubPosition other, PubSubTopic topic);

  long diff(PubSubPosition other, PubSubTopic topic);

  byte[] toBytes();

  boolean equals(Object obj);

  int hashCode();

  // static PubSubPosition fromBytes(byte[] bytes);
}
