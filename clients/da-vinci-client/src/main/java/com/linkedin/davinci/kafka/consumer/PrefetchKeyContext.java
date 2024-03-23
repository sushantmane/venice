package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


public class PrefetchKeyContext {
  private PubSubTopicPartition topicPartition;
  private KafkaKey kafkaKey;
  private String triggeredBySharedConsumerId;
  private ByteArrayKey byteArrayKey;
  private MessageType msgType;

  public PrefetchKeyContext(
      PubSubTopicPartition topicPartition,
      KafkaKey kafkaKey,
      MessageType msgType,
      String triggeredBySharedConsumerId) {
    this.topicPartition = topicPartition;
    this.kafkaKey = kafkaKey;
    this.msgType = msgType;
    this.triggeredBySharedConsumerId = triggeredBySharedConsumerId;
    this.byteArrayKey = ByteArrayKey.wrap(kafkaKey.getKey());
  }

  public PubSubTopicPartition getTopicPartition() {
    return topicPartition;
  }

  public KafkaKey getKafkaKey() {
    return kafkaKey;
  }

  public ByteArrayKey getByteArrayKey() {
    return byteArrayKey;
  }

  public MessageType getMsgType() {
    return msgType;
  }

  @Override
  public int hashCode() {
    return byteArrayKey.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PrefetchKeyContext && byteArrayKey.equals(((PrefetchKeyContext) obj).byteArrayKey);
  }
}
