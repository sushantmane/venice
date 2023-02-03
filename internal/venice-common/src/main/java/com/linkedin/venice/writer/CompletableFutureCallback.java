package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.ProduceResult;
import com.linkedin.venice.pubsub.api.PubsubProducerCallback;
import java.util.concurrent.CompletableFuture;


/**
 * Compose a CompletableFuture and Callback together to be a {@code CompletableFutureCallback} type.
 * When the {@code CompletableFutureCallback} is called, the {@code CompletableFuture} internal state will be
 * changed and the callback will be called. The caller can pass a {@code CompletableFutureCallback} to a function
 * accepting a {@code Callback} parameter to get a {@code CompletableFuture} after the function returns.
 */
public class CompletableFutureCallback implements PubsubProducerCallback {
  private final CompletableFuture<Void> completableFuture;
  private PubsubProducerCallback callback = null;

  public CompletableFutureCallback(CompletableFuture<Void> completableFuture) {
    this.completableFuture = completableFuture;
  }

  @Override
  public void onCompletion(ProduceResult produceResult, Exception e) {
    callback.onCompletion(produceResult, e);
    if (e == null) {
      completableFuture.complete(null);
    } else {
      completableFuture.completeExceptionally(e);
    }
  }

  public PubsubProducerCallback getCallback() {
    return callback;
  }

  public void setCallback(PubsubProducerCallback callback) {
    this.callback = callback;
  }
}
