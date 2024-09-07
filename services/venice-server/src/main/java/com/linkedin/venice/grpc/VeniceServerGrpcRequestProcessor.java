package com.linkedin.venice.grpc;

public class VeniceServerGrpcRequestProcessor {
  private VeniceServerGrpcHandler head = null;

  public VeniceServerGrpcRequestProcessor() {
  }

  public void addHandler(VeniceServerGrpcHandler handler) {
    if (head == null) {
      head = handler;
      return;
    }

    VeniceServerGrpcHandler current = head;
    while (current.getNext() != null) {
      current = current.getNext();
    }

    current.addNextHandler(handler);
  }

  public void process(GrpcRequestContext ctx) {
    head.processRequest(ctx);
  }
}
