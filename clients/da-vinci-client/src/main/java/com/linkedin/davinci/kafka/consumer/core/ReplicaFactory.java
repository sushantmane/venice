package com.linkedin.davinci.kafka.consumer.core;

import com.linkedin.davinci.kafka.consumer.core.Replica.ReplicaBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Factory for creating replicas.
 * There should be one and only one {@link ReplicaFactory} per {@link com.linkedin.davinci.kafka.consumer.StoreIngestionService}
 */
public class ReplicaFactory {
  private static final Logger LOGGER = LogManager.getLogger(ReplicaFactory.class);
  private Map<String, Replica> replicaMap = new ConcurrentHashMap<>(32);

  private ReplicaFactory() {
  }

  public Replica createReplica(final String versionedStoreName, final int partitionId) {
    final String replicaName = getReplicaName(versionedStoreName, partitionId);
    return replicaMap.computeIfAbsent(replicaName, k -> {
      System.out.println("Creating replica for " + replicaName);
      ReplicaBuilder replicaBuilder = new ReplicaBuilder(versionedStoreName, partitionId);
      return replicaBuilder.build();
    });
  }

  public static String getReplicaName(String versionedStoreName, int partitionId) {
    return versionedStoreName + "-" + partitionId;
  }

  // builder for ReplicaFactory
  public static class ReplicaFactoryBuilder {
    public ReplicaFactory build() {
      return new ReplicaFactory();
    }
  }
}
