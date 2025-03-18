package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;


public class ApacheKafkaConsumerAdapterFactory implements PubSubConsumerAdapterFactory<PubSubConsumerAdapter> {
  private static final String NAME = "ApacheKafkaConsumerAdapter";

  @Override
  public ApacheKafkaConsumerAdapter create(
      VeniceProperties veniceProperties,
      boolean isKafkaConsumerOffsetCollectionEnabled,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      String consumerName) {
    PubSubConsumerAdapterContext context =
        new PubSubConsumerAdapterContext.Builder().setVeniceProperties(veniceProperties)
            .setConsumerName(consumerName)
            .setPubSubMessageDeserializer(pubSubMessageDeserializer)
            .setIsOffsetCollectionEnabled(isKafkaConsumerOffsetCollectionEnabled)
            .build();
    return create(context);
  }

  @Override
  public ApacheKafkaConsumerAdapter create(PubSubConsumerAdapterContext context) {
    return new ApacheKafkaConsumerAdapter(new ApacheKafkaConsumerConfig(context));
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() throws IOException {
  }
}
