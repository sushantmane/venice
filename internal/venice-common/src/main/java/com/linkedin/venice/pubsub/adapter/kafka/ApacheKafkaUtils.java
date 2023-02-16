package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import org.apache.kafka.common.header.internals.RecordHeaders;


public class ApacheKafkaUtils {
  public static RecordHeaders convertToKafkaSpecificHeaders(PubSubMessageHeaders headers) {
    RecordHeaders recordHeaders = new RecordHeaders();
    if (headers != null) {
      headers.toList().forEach(header -> recordHeaders.add(header.key(), header.value()));
    }
    return recordHeaders;
  }
}
