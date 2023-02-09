package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.PubsubMessageHeaders;
import org.apache.kafka.common.header.internals.RecordHeaders;


public class ApacheKafkaRecordHeaders extends RecordHeaders {
  public ApacheKafkaRecordHeaders(PubsubMessageHeaders headers) {
    headers.toList().forEach(header -> add(header.key(), header.value()));
  }
}
