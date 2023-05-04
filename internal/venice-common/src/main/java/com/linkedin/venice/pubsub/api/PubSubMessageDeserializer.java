package com.linkedin.venice.pubsub.api;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.pools.ObjectPool;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class should be extended by all pubsub message deserializers to convert from the pubsub specific
 * message format to PubSubMessage.
 */
public abstract class PubSubMessageDeserializer<POSITION> {
  private static final Logger LOGGER = LogManager.getLogger(PubSubMessageDeserializer.class);

  public static final String VENICE_TRANSPORT_PROTOCOL_HEADER = "vtp";

  private final KafkaKeySerializer keySerializer = new KafkaKeySerializer();
  private final KafkaValueSerializer valueSerializer;
  private final ObjectPool<KafkaMessageEnvelope> putEnvelopePool;
  private final ObjectPool<KafkaMessageEnvelope> updateEnvelopePool;

  public PubSubMessageDeserializer(
      KafkaValueSerializer valueSerializer,
      ObjectPool<KafkaMessageEnvelope> putEnvelopePool,
      ObjectPool<KafkaMessageEnvelope> updateEnvelopePool) {
    this.valueSerializer = valueSerializer;
    this.putEnvelopePool = putEnvelopePool;
    this.updateEnvelopePool = updateEnvelopePool;
  }

  public PubSubMessage<KafkaKey, KafkaMessageEnvelope, POSITION> deserialize(
      PubSubTopicPartition topicPartition,
      byte[] keyBytes,
      byte[] valueBytes,
      PubSubMessageHeaders headers,
      POSITION position,
      Long timestamp) {
    // TODO: Put the key in an object pool as well
    KafkaKey key = keySerializer.deserialize(null, valueBytes);
    KafkaMessageEnvelope value = null;
    if (key.isControlMessage()) {
      for (PubSubMessageHeader header: headers.toList()) {
        if (header.key().equals(VENICE_TRANSPORT_PROTOCOL_HEADER)) {
          try {
            Schema providedProtocolSchema = AvroCompatibilityHelper.parse(new String(header.value()));
            value =
                valueSerializer.deserialize(valueBytes, providedProtocolSchema, getEnvelope(key.getKeyHeaderByte()));
          } catch (Exception e) {
            // Improper header... will ignore.
            LOGGER.warn("Received unparsable schema in protocol header: " + VENICE_TRANSPORT_PROTOCOL_HEADER, e);
          }
          break; // We don't look at other headers
        }
      }
    }
    if (value == null) {
      value = valueSerializer.deserialize(valueBytes, getEnvelope(key.getKeyHeaderByte()));
    }
    // TODO: Put the message container in an object pool as well
    return new ImmutablePubSubMessage<>(
        key,
        value,
        topicPartition,
        position,
        timestamp,
        keyBytes.length + valueBytes.length);
  }

  private KafkaMessageEnvelope getEnvelope(byte keyHeaderByte) {
    switch (keyHeaderByte) {
      case MessageType.Constants.PUT_KEY_HEADER_BYTE:
        return putEnvelopePool.get();
      // No need to pool control messages since there are so few of them, and they are varied anyway, limiting reuse.
      case MessageType.Constants.CONTROL_MESSAGE_KEY_HEADER_BYTE:
        return new KafkaMessageEnvelope();
      case MessageType.Constants.UPDATE_KEY_HEADER_BYTE:
        return updateEnvelopePool.get();
      default:
        throw new IllegalStateException("Illegal key header byte: " + keyHeaderByte);
    }
  }
}
