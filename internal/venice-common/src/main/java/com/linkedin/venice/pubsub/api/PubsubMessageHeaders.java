package com.linkedin.venice.pubsub.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Set of key-value pairs to tagged with messages produced to a topic.
 * In case of headers with the same key, only the most recently added headers value will be kept.
 */
public class PubsubMessageHeaders {

  // Kafka allows duplicate keys in the headers but some pubsub systems may not
  // allow it. Hence, it would be good to enforce uniqueness of keys in headers
  // from the beginning.
  private final Map<String, PubsubMessageHeader> headers = new LinkedHashMap<>();

  public PubsubMessageHeaders add(PubsubMessageHeader header) {
    headers.put(header.key(), header);
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
   * @return the headers as a List<PubsubMessageHeader>.
   *    Mutating this list will not affect the PubsubMessageHeaders.
   *    If no headers are present an empty list is returned.
   */
  public List<PubsubMessageHeader> toList() {
    return new ArrayList<>(headers.values());
  }
}
