package com.linkedin.venice.pubsub.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class PubsubMessageHeaders {

  // Kafka allows duplicate keys in the headers but some pubsub systems may not
  // allow it. Hence, it would be good to enforce uniqueness of keys in headers
  // from the beginning.
  private final Map<String, PubsubMessageHeader> headers = new LinkedHashMap<>();

  public PubsubMessageHeaders add(PubsubMessageHeader header) {
    this.headers.put(header.key(), header);
    return this;
  }

  public PubsubMessageHeaders add(String key, byte[] value) {
    add(new PubsubMessageHeader(key, value));
    return this;
  }

  public PubsubMessageHeaders remove(String key) {
    headers.remove(key);
    return this;
  }

  /**
   * Returns all headers as a List, in the order they were added in.
   *
   * @return the headers as a List<PubsubMessageHeader>, mutating this list will not affect the PubsubMessageHeaders, if NO headers are present an empty list is returned.
   */
  public List<PubsubMessageHeader> toList() {
    return new ArrayList<>(headers.values());
  }
}
