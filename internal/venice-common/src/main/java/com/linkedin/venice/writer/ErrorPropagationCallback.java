package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.ProduceResult;
import com.linkedin.venice.pubsub.api.PubsubProducerCallback;


/**
 * Wraps another {@link PubsubProducerCallback} and propagates exceptions to it, but swallows successful completions.
 */
class ErrorPropagationCallback implements PubsubProducerCallback {
  private final PubsubProducerCallback callback;

  public ErrorPropagationCallback(PubsubProducerCallback callback) {
    this.callback = callback;
  }

  @Override
  public void onCompletion(ProduceResult produceResult, Exception exception) {
    if (exception != null) {
      callback.onCompletion(null, exception);
    } // else, no-op
  }
}
