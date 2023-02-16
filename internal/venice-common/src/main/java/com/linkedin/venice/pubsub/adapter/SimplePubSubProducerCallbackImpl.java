package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubsubProduceResult;


/**
 * A simple implementation of PubSubProducerCallback interface for testing purposes.
 */
public class SimplePubSubProducerCallbackImpl implements PubSubProducerCallback {
  private PubsubProduceResult produceResult;
  private Exception exception;
  private boolean isInvoked;

  @Override
  public void onCompletion(PubsubProduceResult produceResult, Exception exception) {
    this.isInvoked = true;
    this.produceResult = produceResult;
    this.exception = exception;
  }

  public boolean isInvoked() {
    return isInvoked;
  }

  public PubsubProduceResult getProduceResult() {
    return produceResult;
  }

  public Exception getException() {
    return exception;
  }
}
