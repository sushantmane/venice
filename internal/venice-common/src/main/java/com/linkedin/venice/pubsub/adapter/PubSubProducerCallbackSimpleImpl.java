package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A simple implementation of PubSubProducerCallback interface for testing purposes.
 */
public class PubSubProducerCallbackSimpleImpl implements PubSubProducerCallback {
  private static final Logger LOGGER = LogManager.getLogger(PubSubProducerCallbackSimpleImpl.class);
  private PubSubProduceResult produceResult;
  private Exception exception;
  private boolean isInvoked;

  @Override
  public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
    this.produceResult = produceResult;
    this.exception = exception;
    this.isInvoked = true;
    LOGGER.info("{} has been invoked. ProduceResult:{}, Exception:{}", this, produceResult, exception);
  }

  public boolean isInvoked() {
    return isInvoked;
  }

  public PubSubProduceResult getProduceResult() {
    return produceResult;
  }

  public Exception getException() {
    return exception;
  }
}
