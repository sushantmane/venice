package com.linkedin.davinci.kafka.consumer.core;

import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Replica {
  private static Logger logger = LogManager.getLogger(Replica.class);
  private final ReplicaBuilder context;
  private final StoreIngestionTask storeIngestionTask;
  /**
   * The number of messages that are consumed from the topic but are not yet persisted in db or have been ignored.
   *
   */
  private final Map<RegionalPoolKey, AtomicInteger> inflightMessages = new ConcurrentHashMap<>(3);

  private Replica(ReplicaBuilder context) {
    this.context = context;
    this.storeIngestionTask = context.storeIngestionTask;
  }

  public static class ReplicaBuilder {
    private String replicaName;
    private String versionedStoreName;
    private int partitionId;
    private StoreIngestionTask storeIngestionTask;

    public ReplicaBuilder(String versionedStoreName, int partitionId) {
      this.versionedStoreName = Objects.requireNonNull(versionedStoreName);
      this.partitionId = partitionId;
    }

    public ReplicaBuilder setReplicaName(String replicaName) {
      this.replicaName = Objects.requireNonNull(replicaName, "Replica name cannot be null");
      return this;
    }

    public ReplicaBuilder setStoreIngestionTask(StoreIngestionTask storeIngestionTask) {
      this.storeIngestionTask = Objects.requireNonNull(storeIngestionTask, "Store ingestion task cannot be null");
      return this;
    }

    public Replica build() {
      if (replicaName == null) {
        replicaName = versionedStoreName + "-" + partitionId;
      }
      return new Replica(this);
    }
  }
}
