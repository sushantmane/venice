package com.linkedin.davinci.stats;

import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.RegionUtils;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMaps;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AggLagStats extends AbstractVeniceStats implements Closeable {
  public static final Logger LOGGER = LogManager.getLogger(AggLagStats.class);
  private final ScheduledExecutorService scheduler;
  private final StoreIngestionService storeIngestionService;
  private final Int2ObjectMap<String> kafkaClusterIdToAliasMap;
  private final Int2LongMap aggRegionHybridOffsetLagTotalMap;
  private volatile long aggBatchReplicationLagFuture;
  private volatile long aggBatchLeaderOffsetLagFuture;
  private volatile long aggBatchFollowerOffsetLagFuture;
  private volatile long aggHybridLeaderOffsetLagTotal;
  private volatile long aggHybridFollowerOffsetLagTotal;

  public AggLagStats(StoreIngestionService storeIngestionService, MetricsRepository metricsRepository) {
    super(metricsRepository, "AggLagStats");
    this.storeIngestionService = storeIngestionService;
    this.kafkaClusterIdToAliasMap =
        storeIngestionService.getVeniceConfigLoader().getVeniceServerConfig().getKafkaClusterIdToAliasMap();
    this.aggRegionHybridOffsetLagTotalMap =
        Int2LongMaps.synchronize(new Int2LongOpenHashMap(kafkaClusterIdToAliasMap.size()));
    for (Int2ObjectMap.Entry<String> entry: kafkaClusterIdToAliasMap.int2ObjectEntrySet()) {
      String regionNamePrefix = RegionUtils.getRegionSpecificMetricPrefix(
          storeIngestionService.getVeniceConfigLoader().getVeniceServerConfig().getRegionName(),
          entry.getValue());
      registerSensor(
          regionNamePrefix + "_rt_lag",
          new Gauge(() -> getAggRegionHybridOffsetLagTotal(entry.getIntKey())));
    }
    registerSensor("agg_batch_replication_lag_future", new Gauge(this::getAggBatchReplicationLagFuture));
    registerSensor("agg_batch_leader_offset_lag_future", new Gauge(this::getAggBatchLeaderOffsetLagFuture));
    registerSensor("agg_batch_follower_offset_lag_future", new Gauge(this::getAggBatchFollowerOffsetLagFuture));
    registerSensor("agg_hybrid_leader_offset_lag_total", new Gauge(this::getAggHybridLeaderOffsetLagTotal));
    registerSensor("agg_hybrid_follower_offset_lag_total", new Gauge(this::getAggHybridFollowerOffsetLagTotal));
    this.scheduler = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("AggLagStats"));
  }

  public void startLagCollector() {
    Runnable lagCollector = () -> {
      try {
        collectLagForAllStoreIngestionTasks();
      } catch (Exception e) {
        LOGGER.warn("Failed to collect all lags", e);
      }
    };
    scheduler.scheduleWithFixedDelay(lagCollector, 0, 1, TimeUnit.MINUTES);
    LOGGER.info("Started lag collector");
  }

  // This method is called every minute to collect lag for all store ingestion tasks.
  private void collectLagForAllStoreIngestionTasks() {
    long aggBatchReplicationLagFuture = 0;
    long aggBatchLeaderOffsetLagFuture = 0;
    long aggBatchFollowerOffsetLagFuture = 0;
    long aggHybridLeaderOffsetLagTotal = 0;
    long aggHybridFollowerOffsetLagTotal = 0;
    Map<String, StoreIngestionTask> storeIngestionTaskMap = storeIngestionService.getStoreIngestionTasks();
    for (Map.Entry<String, StoreIngestionTask> entry: storeIngestionTaskMap.entrySet()) {
      StoreIngestionTask storeIngestionTask = entry.getValue();
      if (storeIngestionTask.isFutureVersion()) {
        aggBatchReplicationLagFuture += storeIngestionTask.getBatchReplicationLag();
        aggBatchLeaderOffsetLagFuture += storeIngestionTask.getBatchLeaderOffsetLag();
        aggBatchFollowerOffsetLagFuture += storeIngestionTask.getBatchFollowerOffsetLag();
      }

      aggHybridLeaderOffsetLagTotal += storeIngestionTask.getHybridLeaderOffsetLag();
      aggHybridFollowerOffsetLagTotal += storeIngestionTask.getHybridFollowerOffsetLag();
    }
    this.aggBatchReplicationLagFuture = aggBatchReplicationLagFuture;
    this.aggBatchLeaderOffsetLagFuture = aggBatchLeaderOffsetLagFuture;
    this.aggBatchFollowerOffsetLagFuture = aggBatchFollowerOffsetLagFuture;
    this.aggHybridLeaderOffsetLagTotal = aggHybridLeaderOffsetLagTotal;
    this.aggHybridFollowerOffsetLagTotal = aggHybridFollowerOffsetLagTotal;

    for (int regionId: kafkaClusterIdToAliasMap.keySet()) {
      long aggRegionHybridOffsetLagTotal = 0;
      for (Map.Entry<String, StoreIngestionTask> entry: storeIngestionTaskMap.entrySet()) {
        StoreIngestionTask storeIngestionTask = entry.getValue();
        aggRegionHybridOffsetLagTotal += storeIngestionTask.getRegionHybridOffsetLag(regionId);
      }
      aggRegionHybridOffsetLagTotalMap.put(regionId, aggRegionHybridOffsetLagTotal);
    }
  }

  public final long getAggBatchReplicationLagFuture() {
    return aggBatchReplicationLagFuture;
  }

  public final long getAggBatchLeaderOffsetLagFuture() {
    return aggBatchLeaderOffsetLagFuture;
  }

  public final long getAggBatchFollowerOffsetLagFuture() {
    return aggBatchFollowerOffsetLagFuture;
  }

  public final long getAggHybridLeaderOffsetLagTotal() {
    return aggHybridLeaderOffsetLagTotal;
  }

  public final long getAggHybridFollowerOffsetLagTotal() {
    return aggHybridFollowerOffsetLagTotal;
  }

  public final long getAggRegionHybridOffsetLagTotal(int regionId) {
    return aggRegionHybridOffsetLagTotalMap.getOrDefault(regionId, 0L);
  }

  @Override
  public void close() throws IOException {
    scheduler.shutdown();
    try {
      scheduler.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Failed to shutdown lag collector", e);
    }
    LOGGER.info("Stopped lag collector");
  }
}
