package com.linkedin.venice.pubsub.api;

public interface PubsubProducerCallback {
  void onCompletion(ProduceResult metadata, Exception exception);
}
