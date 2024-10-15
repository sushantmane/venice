package com.linkedin.venice.fastclient.transport.grpc;

import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.fastclient.TransportResponseHandler;
import com.linkedin.venice.protocols.MultiKeyStreamingResponse;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MultiKeyStreamingResponseObserver implements StreamObserver<MultiKeyStreamingResponse> {
  private static final Logger LOGGER = LogManager.getLogger(MultiKeyStreamingResponseObserver.class);

  private final CompletableFuture<TransportClientResponse> future;
  private final TransportResponseHandler transportResponseHandler;

  // used mainly for testing
  MultiKeyStreamingResponseObserver(
      TransportResponseHandler transportResponseHandler,
      CompletableFuture<TransportClientResponse> future) {
    this.transportResponseHandler = transportResponseHandler;
    this.future = future;
  }

  public MultiKeyStreamingResponseObserver(TransportResponseHandler transportResponseHandler) {
    this(transportResponseHandler, new CompletableFuture<>());
  }

  public CompletableFuture<TransportClientResponse> getFuture() {
    return future;
  }

  @Override
  public void onNext(MultiKeyStreamingResponse value) {

  }

  @Override
  public void onError(Throwable t) {

  }

  @Override
  public void onCompleted() {

  }
}
