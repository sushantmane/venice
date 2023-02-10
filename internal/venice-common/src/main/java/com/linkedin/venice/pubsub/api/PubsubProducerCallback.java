package com.linkedin.venice.pubsub.api;

/**
 * A callback interface that users of ProducerAdapter can implement if they want
 * to execute some code once ProducerAdapter#sendMessage request is completed.
 */
public interface PubsubProducerCallback {
  /**
   * exception will be null if request was completed without an error.
   */
  void onCompletion(ProduceResult produceResult, Exception exception);
}
