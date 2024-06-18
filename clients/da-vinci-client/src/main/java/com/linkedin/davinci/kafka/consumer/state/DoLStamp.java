package com.linkedin.davinci.kafka.consumer.state;

import com.linkedin.venice.pubsub.api.PubSubMessageHeader;


/**
 * Declaration of Leadership Stamp (DoLStamp) is a record sent by the leader to let followers know
 * that it is taking over the leadership of the partition.
 */
public class DoLStamp {
  public static final String DOL_STAMP = "DoLStamp";
  private final long termId;

  public DoLStamp(long termId) {
    this.termId = termId;
  }

  public long getTermId() {
    return termId;
  }

  PubSubMessageHeader getHeader() {
    return new PubSubMessageHeader(DOL_STAMP, Long.toString(termId).getBytes());
  }
}
