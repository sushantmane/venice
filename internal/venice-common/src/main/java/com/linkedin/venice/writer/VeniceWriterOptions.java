package com.linkedin.venice.writer;

import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;


/**
 * VeniceWriterOptions is used to pass arguments to VeniceWriter constructor.
 * Before passing VeniceWriterOptions object to VeniceWriter, VeniceWriterFactory
 * methods use it to set some configs.
 */
public class VeniceWriterOptions {
  private final String topicName;
  // TODO: Update to use generic serializers
  private final VeniceKafkaSerializer keySerializer;
  private final VeniceKafkaSerializer valueSerializer;
  private final VeniceKafkaSerializer writeComputeSerializer;
  private final VenicePartitioner partitioner;
  private final Time time;
  private final Integer partitionCount;
  private final boolean chunkingEnabled;
  private final boolean rmdChunkingEnabled;
  // Set this field if you do not wish to use default broker address of producer factory
  private final String brokerAddress;

  public String getBrokerAddress() {
    return brokerAddress;
  }

  public String getTopicName() {
    return topicName;
  }

  public VeniceKafkaSerializer getKeySerializer() {
    return keySerializer;
  }

  public VeniceKafkaSerializer getValueSerializer() {
    return valueSerializer;
  }

  public VeniceKafkaSerializer getWriteComputeSerializer() {
    return writeComputeSerializer;
  }

  public VenicePartitioner getPartitioner() {
    return partitioner;
  }

  public Time getTime() {
    return time;
  }

  public Integer getPartitionCount() {
    return partitionCount;
  }

  public boolean isChunkingEnabled() {
    return chunkingEnabled;
  }

  public boolean isRmdChunkingEnabled() {
    return rmdChunkingEnabled;
  }

  private VeniceWriterOptions(Builder builder) {
    topicName = builder.topicName;
    keySerializer = builder.keySerializer;
    valueSerializer = builder.valueSerializer;
    writeComputeSerializer = builder.writeComputeSerializer;
    partitioner = builder.partitioner;
    time = builder.time;
    partitionCount = builder.partitionCount;
    chunkingEnabled = builder.chunkingEnabled;
    rmdChunkingEnabled = builder.rmdChunkingEnabled;
    brokerAddress = builder.brokerAddress;
  }

  @Override
  public String toString() {
    return new StringBuilder("VeniceWriterOptions:{").append("topic:")
        .append(topicName)
        .append(", ")
        .append("brokerAddress:")
        .append(brokerAddress)
        .append(", ")
        .append("chunkingEnabled:")
        .append(chunkingEnabled)
        .append(", ")
        .append("partitionCount:")
        .append(partitionCount != null ? partitionCount : "-")
        .append("}")
        .toString();
  }

  public static class Builder {
    private final String topicName;
    private VeniceKafkaSerializer keySerializer = null;
    private VeniceKafkaSerializer valueSerializer = null;
    private VeniceKafkaSerializer writeComputeSerializer = null;
    private VenicePartitioner partitioner = null;
    private Time time = null;
    private Integer partitionCount = null;
    private boolean chunkingEnabled;
    private boolean rmdChunkingEnabled;
    private boolean useKafkaKeySerializer = false;
    private String brokerAddress = null;

    private void addDefaults() {
      if (keySerializer == null) {
        keySerializer = new DefaultSerializer();
      }
      if (valueSerializer == null) {
        valueSerializer = new DefaultSerializer();
      }
      if (writeComputeSerializer == null) {
        writeComputeSerializer = new DefaultSerializer();
      }
      if (partitioner == null) {
        partitioner = new DefaultVenicePartitioner();
      }
      if (time == null) {
        time = SystemTime.INSTANCE;
      }
    }

    public VeniceWriterOptions build() {
      addDefaults();
      return new VeniceWriterOptions(this);
    }

    public String getBrokerAddress() {
      return brokerAddress;
    }

    public Builder setBrokerAddress(String brokerAddress) {
      this.brokerAddress = brokerAddress;
      return this;
    }

    public boolean isUseKafkaKeySerializer() {
      return useKafkaKeySerializer;
    }

    public Builder setUseKafkaKeySerializer(boolean useKafkaKeySerializer) {
      if (useKafkaKeySerializer) {
        this.keySerializer = new KafkaKeySerializer();
      }
      this.useKafkaKeySerializer = useKafkaKeySerializer;
      return this;
    }

    public boolean isChunkingEnabled() {
      return chunkingEnabled;
    }

    public Builder setChunkingEnabled(boolean chunkingEnabled) {
      this.chunkingEnabled = chunkingEnabled;
      return this;
    }

    public boolean isRmdChunkingEnabled() {
      return rmdChunkingEnabled;
    }

    public Builder setRmdChunkingEnabled(boolean rmdChunkingEnabled) {
      this.rmdChunkingEnabled = rmdChunkingEnabled;
      return this;
    }

    public Builder(String topic) {
      this.topicName = topic;
    }

    public String getTopicName() {
      return topicName;
    }

    public VeniceKafkaSerializer getKeySerializer() {
      return keySerializer;
    }

    public Builder setKeySerializer(VeniceKafkaSerializer keySerializer) {
      this.keySerializer = keySerializer;
      return this;
    }

    public VeniceKafkaSerializer getValueSerializer() {
      return valueSerializer;
    }

    public Builder setValueSerializer(VeniceKafkaSerializer valueSerializer) {
      this.valueSerializer = valueSerializer;
      return this;
    }

    public VeniceKafkaSerializer getWriteComputeSerializer() {
      return writeComputeSerializer;
    }

    public Builder setWriteComputeSerializer(VeniceKafkaSerializer writeComputeSerializer) {
      this.writeComputeSerializer = writeComputeSerializer;
      return this;
    }

    public VenicePartitioner getPartitioner() {
      return partitioner;
    }

    public Builder setPartitioner(VenicePartitioner partitioner) {
      this.partitioner = partitioner;
      return this;
    }

    public Time getTime() {
      return time;
    }

    public Builder setTime(Time time) {
      this.time = time;
      return this;
    }

    public Integer getPartitionCount() {
      return partitionCount;
    }

    public Builder setPartitionCount(Integer partitionCount) {
      this.partitionCount = partitionCount;
      return this;
    }
  }
}
