package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubsubProduceResult;


/**
 * Wraps another {@link PubSubProducerCallback} and propagates exceptions to it, but swallows successful completions.
 */
class ErrorPropagationCallback implements PubSubProducerCallback {
  private final PubSubProducerCallback callback;

  public ErrorPropagationCallback(PubSubProducerCallback callback) {
    this.callback = callback;
  }

  @Override
  public void onCompletion(PubsubProduceResult produceResult, Exception exception) {
    if (exception != null) {
      callback.onCompletion(null, exception);
    } // else, no-op
  }
}
