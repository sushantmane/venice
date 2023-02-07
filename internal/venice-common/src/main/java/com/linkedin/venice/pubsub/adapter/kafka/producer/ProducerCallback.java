package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.ProduceResult;
import com.linkedin.venice.pubsub.api.PubsubProducerCallback;
import java.util.concurrent.CompletableFuture;


public abstract class ProducerCallback implements PubsubProducerCallback {
  private CompletableFuture<ProduceResult> producerResultFuture = new CompletableFuture<>();

  void complete(ProduceResult produceResult, Exception exception) {
    if (exception != null) {
      producerResultFuture.completeExceptionally(exception);
      return;
    }
    producerResultFuture.complete(produceResult);
  }
}
