package com.linkedin.venice.listener;

public class StatusBasedReorderingQueue {
  enum Status {
    OK, ERROR
  }

  // pojo: status and timestamp in nanoseconds
  public static class StatusAndTimestamp {
    public Status status;
    public long timestamp;

    public StatusAndTimestamp(Status status, long timestamp) {
      this.status = status;
      this.timestamp = timestamp;
    }
  }

}
