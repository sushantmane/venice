package com.linkedin.venice.grpc;

import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.listener.NoOpReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.QuotaEnforcementHandler;
import com.linkedin.venice.listener.StatsHandler;
import com.linkedin.venice.listener.StorageReadRequestHandler;


public class GrpcServiceDependencies {
  private final DiskHealthCheckService diskHealthCheckService;
  private final StorageReadRequestHandler storageReadRequestHandler;
  private final QuotaEnforcementHandler quotaEnforcementHandler;
  private final StatsHandler statsHandler;

  private GrpcServiceDependencies(Builder builder) {
    this.diskHealthCheckService = builder.diskHealthCheckService;
    this.storageReadRequestHandler = builder.storageReadRequestHandler;
    this.quotaEnforcementHandler = builder.quotaEnforcementHandler;
    this.statsHandler = builder.statsHandler;
  }

  public DiskHealthCheckService getDiskHealthCheckService() {
    return diskHealthCheckService;
  }

  public StorageReadRequestHandler getStorageReadRequestHandler() {
    return storageReadRequestHandler;
  }

  public QuotaEnforcementHandler getQuotaEnforcementHandler() {
    return quotaEnforcementHandler;
  }

  public StatsHandler getStatsHandler() {
    return statsHandler;
  }

  public static class Builder {
    private DiskHealthCheckService diskHealthCheckService;
    private StorageReadRequestHandler storageReadRequestHandler;
    private QuotaEnforcementHandler quotaEnforcementHandler;
    private StatsHandler statsHandler;

    public Builder setDiskHealthCheckService(DiskHealthCheckService diskHealthCheckService) {
      this.diskHealthCheckService = diskHealthCheckService;
      return this;
    }

    public Builder setStorageReadRequestHandler(StorageReadRequestHandler storageReadRequestHandler) {
      this.storageReadRequestHandler = storageReadRequestHandler;
      return this;
    }

    public Builder setQuotaEnforcementHandler(QuotaEnforcementHandler quotaEnforcementHandler) {
      this.quotaEnforcementHandler = quotaEnforcementHandler;
      return this;
    }

    public Builder setStatsHandler(StatsHandler statsHandler) {
      this.statsHandler = statsHandler;
      return this;
    }

    public GrpcServiceDependencies build() {
      if (quotaEnforcementHandler == null) {
        quotaEnforcementHandler = new NoOpReadQuotaEnforcementHandler();
      }

      return new GrpcServiceDependencies(this);
    }
  }
}
