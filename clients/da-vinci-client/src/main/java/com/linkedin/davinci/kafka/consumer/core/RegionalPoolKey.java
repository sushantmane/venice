package com.linkedin.davinci.kafka.consumer.core;

public class RegionalPoolKey {
  String regionName;
  PoolKey poolKey;

  public RegionalPoolKey(String regionName, PoolKey poolKey) {
    this.regionName = regionName;
    this.poolKey = poolKey;
  }

  public static String buildRegionalPoolKey(
      String regionName,
      VersionStatus versionStatus,
      ReplicaRole role,
      Workload workload) {
    return regionName + "-" + PoolKey.getPoolKey(versionStatus, role, workload);
  }
}
