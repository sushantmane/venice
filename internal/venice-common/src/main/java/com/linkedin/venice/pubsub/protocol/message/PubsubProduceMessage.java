package com.linkedin.venice.pubsub.protocol.message;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubsubMessageHeaders;
import java.util.Objects;


public class PubsubProduceMessage {
  private final String topic;
  private final Integer partition;
  private final KafkaKey key;
  private final KafkaMessageEnvelope value;
  private final PubsubMessageHeaders headers;

  public PubsubProduceMessage(Builder builder) {
    this.topic = builder.topic;
    this.partition = builder.partition;
    this.key = builder.key;
    this.value = builder.value;
    this.headers = builder.headers;
  }

  public String getTopic() {
    return topic;
  }

  public Integer getPartition() {
    return partition;
  }

  public KafkaKey getKey() {
    return key;
  }

  public KafkaMessageEnvelope getValue() {
    return value;
  }

  public PubsubMessageHeaders getHeaders() {
    return headers;
  }

  public static class Builder {
    private String topic;
    private int partition;
    private KafkaKey key;
    private KafkaMessageEnvelope value;
    private PubsubMessageHeaders headers;

    public Builder topic(String topic) {
      this.topic = Objects.requireNonNull(topic, "Topic name cannot be null");
      return this;
    }

    public Builder key(Integer partition) {
      this.partition = partition;
      return this;
    }

    public Builder key(KafkaKey key) {
      this.key = key;
      return this;
    }

    public Builder value(KafkaMessageEnvelope value) {
      this.value = value;
      return this;
    }

    public Builder headers(PubsubMessageHeaders headers) {
      this.headers = headers;
      return this;
    }

    public PubsubProduceMessage build() {
      return new PubsubProduceMessage(this);
    }
  }
}
