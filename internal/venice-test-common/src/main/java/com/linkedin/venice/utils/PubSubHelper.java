package com.linkedin.venice.utils;

import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class PubSubHelper {
  public static KafkaKey getDummyKey() {
    return getDummyKey(false);
  }

  public static KafkaKey getDummyKey(boolean isControlMessage) {
    if (isControlMessage) {
      return new KafkaKey(MessageType.CONTROL_MESSAGE, Utils.getUniqueString("dummyCmKey").getBytes());
    }
    return new KafkaKey(MessageType.PUT, Utils.getUniqueString("dummyKey").getBytes());
  }

  public static KafkaMessageEnvelope getDummyValue() {
    return getDummyValue(System.currentTimeMillis());
  }

  public static KafkaMessageEnvelope getDummyValue(long producerMessageTimestamp) {
    KafkaMessageEnvelope value = new KafkaMessageEnvelope();
    value.producerMetadata = new ProducerMetadata();
    value.producerMetadata.messageTimestamp = producerMessageTimestamp;
    value.producerMetadata.messageSequenceNumber = 0;
    value.producerMetadata.segmentNumber = 0;
    value.producerMetadata.producerGUID = new GUID();
    Put put = new Put();
    put.putValue = ByteBuffer.allocate(1024);
    put.replicationMetadataPayload = ByteBuffer.allocate(0);
    value.payloadUnion = put;
    return value;
  }

  public static MutablePubSubMessage getDummyPubSubMessage(boolean isControlMessage) {
    return new MutablePubSubMessage().setKey(getDummyKey(isControlMessage)).setValue(getDummyValue());
  }

  public static Map<Integer, MutablePubSubMessage> produceMessages(
      PubSubProducerAdapter pubSubProducerAdapter,
      String topicName,
      int partition,
      int messageCount,
      long delayBetweenMessagesInMs) throws InterruptedException, ExecutionException, TimeoutException {
    Map<Integer, MutablePubSubMessage> messages = new ConcurrentHashMap<>(messageCount);

    CompletableFuture<PubSubProduceResult> lastMessageFuture = null;

    for (int i = 0; i < messageCount; i++) {
      MutablePubSubMessage message = PubSubHelper.getDummyPubSubMessage(false);
      message.getValue().getProducerMetadata().setMessageTimestamp(i); // logical ts
      message.setTimestampBeforeProduce(System.currentTimeMillis());
      int finalI = i;
      lastMessageFuture =
          pubSubProducerAdapter.sendMessage(topicName, partition, message.getKey(), message.getValue(), null, null);
      lastMessageFuture.whenComplete((result, throwable) -> {
        if (throwable == null) {
          messages.put(finalI, message);
          message.setOffset(result.getOffset());
          message.setTimestampAfterProduce(System.currentTimeMillis());
        }
      });
      TimeUnit.MILLISECONDS.sleep(delayBetweenMessagesInMs);
    }

    assertNotNull(lastMessageFuture, "Last message future should not be null");
    lastMessageFuture.get(1, TimeUnit.MINUTES);

    return messages;
  }

  // mutable publish-sub message
  public static class MutablePubSubMessage {
    private KafkaKey key;
    private KafkaMessageEnvelope value;
    private PubSubTopicPartition topicPartition;
    private long offset;
    private long timestampBeforeProduce;
    private long timestampAfterProduce;

    public KafkaKey getKey() {
      return key;
    }

    public KafkaMessageEnvelope getValue() {
      return value;
    }

    public PubSubTopicPartition getTopicPartition() {
      return topicPartition;
    }

    public Long getOffset() {
      return offset;
    }

    public MutablePubSubMessage setKey(KafkaKey key) {
      this.key = key;
      return this;
    }

    public MutablePubSubMessage setValue(KafkaMessageEnvelope value) {
      this.value = value;
      return this;
    }

    public MutablePubSubMessage setTopicPartition(PubSubTopicPartition topicPartition) {
      this.topicPartition = topicPartition;
      return this;
    }

    public MutablePubSubMessage setOffset(long offset) {
      this.offset = offset;
      return this;
    }

    public MutablePubSubMessage setTimestampBeforeProduce(long timestampBeforeProduce) {
      this.timestampBeforeProduce = timestampBeforeProduce;
      return this;
    }

    public MutablePubSubMessage setTimestampAfterProduce(long timestampAfterProduce) {
      this.timestampAfterProduce = timestampAfterProduce;
      return this;
    }

    public long getTimestampBeforeProduce() {
      return timestampBeforeProduce;
    }

    public long getTimestampAfterProduce() {
      return timestampAfterProduce;
    }
  }
}
