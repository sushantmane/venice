package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.pubsub.api.PubsubProduceResult;
import com.linkedin.venice.pubsub.api.PubsubProducerCallback;


/**
 * A simple implementation of PubsubProducerCallback interface for testing purposes.
 */
public class SimplePubsubProducerCallbackImpl implements PubsubProducerCallback {
  private PubsubProduceResult producerResult;
  private Exception exception;
  private boolean isInvoked;

  @Override
  public void onCompletion(PubsubProduceResult produceResult, Exception exception) {
    this.isInvoked = true;
    this.producerResult = produceResult;
    this.exception = exception;
  }

  public boolean isInvoked() {
    return isInvoked;
  }

  public PubsubProduceResult producerResult() {
    return producerResult;
  }

  public Exception exception() {
    return exception;
  }
}
