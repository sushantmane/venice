package com.linkedin.venice.pubsub.adapter.kafka.admin;

import com.linkedin.venice.pubsub.PubSubAdminAdapterContext;
import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;


/**
 * Implementation of {@link PubSubProducerAdapterFactory} used to create Apache Kafka admin clients.
 *
 * A kafka based admin client created using this factory is for managing and inspecting topics, brokers, configurations and ACLs.
 */

public class ApacheKafkaAdminAdapterFactory implements PubSubAdminAdapterFactory<PubSubAdminAdapter> {
  private static final String NAME = "ApacheKafkaAdmin";

  @Override
  public PubSubAdminAdapter create(VeniceProperties veniceProperties, PubSubTopicRepository pubSubTopicRepository) {
    PubSubAdminAdapterContext context = new PubSubAdminAdapterContext.Builder()
        .setPubSubBrokerAddress(PubSubUtil.getPubSubBrokerAddress(veniceProperties))
        .setProperties(veniceProperties)
        .setPubSubTopicRepository(pubSubTopicRepository)
        .build();
    return create(context);
  }

  @Override
  public PubSubAdminAdapter create(PubSubAdminAdapterContext context) {
    ApacheKafkaAdminConfig apacheKafkaAdminConfig = new ApacheKafkaAdminConfig(context);
    return new ApacheKafkaAdminAdapter(apacheKafkaAdminConfig, context.getPubSubTopicRepository());
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() throws IOException {
  }
}
