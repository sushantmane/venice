package com.linkedin.venice.grpc;

import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.listener.ReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.StatsHandler;
import com.linkedin.venice.listener.StorageReadRequestHandler;


public class GrpcServiceDependencies {
  private final DiskHealthCheckService diskHealthCheckService;
  private final StorageReadRequestHandler storageReadRequestHandler;
  private final ReadQuotaEnforcementHandler readQuotaEnforcementHandler;
  private final StatsHandler statsHandler;

  private GrpcServiceDependencies(Builder builder) {
    this.diskHealthCheckService = builder.diskHealthCheckService;
    this.storageReadRequestHandler = builder.storageReadRequestHandler;
    this.readQuotaEnforcementHandler = builder.readQuotaEnforcementHandler;
    this.statsHandler = builder.statsHandler;
  }

  public DiskHealthCheckService getDiskHealthCheckService() {
    return diskHealthCheckService;
  }

  public StorageReadRequestHandler getStorageReadRequestHandler() {
    return storageReadRequestHandler;
  }

  public ReadQuotaEnforcementHandler getReadQuotaEnforcementHandler() {
    return readQuotaEnforcementHandler;
  }

  public StatsHandler getStatsHandler() {
    return statsHandler;
  }

  public static class Builder {
    private DiskHealthCheckService diskHealthCheckService;
    private StorageReadRequestHandler storageReadRequestHandler;
    private ReadQuotaEnforcementHandler readQuotaEnforcementHandler;
    private StatsHandler statsHandler;

    public Builder setDiskHealthCheckService(DiskHealthCheckService diskHealthCheckService) {
      this.diskHealthCheckService = diskHealthCheckService;
      return this;
    }

    public Builder setStorageReadRequestHandler(StorageReadRequestHandler storageReadRequestHandler) {
      this.storageReadRequestHandler = storageReadRequestHandler;
      return this;
    }

    public Builder setReadQuotaEnforcementHandler(ReadQuotaEnforcementHandler readQuotaEnforcementHandler) {
      this.readQuotaEnforcementHandler = readQuotaEnforcementHandler;
      return this;
    }

    public Builder setStatsHandler(StatsHandler statsHandler) {
      this.statsHandler = statsHandler;
      return this;
    }

    public GrpcServiceDependencies build() {
      return new GrpcServiceDependencies(this);
    }
  }
}
