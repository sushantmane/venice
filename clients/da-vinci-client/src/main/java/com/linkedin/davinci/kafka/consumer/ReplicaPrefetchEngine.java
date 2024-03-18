package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ReplicaPrefetchEngine {
  private static final Logger LOGGER = LogManager.getLogger(ReplicaPrefetchEngine.class);
  static final RecordPrefetchContext EMPTY_RPC = new RecordPrefetchContext(null, -1, Collections.emptyList());
  private static final ThreadPoolExecutor prefetchEngineExecutor = getThreadPoolExecutor();

  private static ThreadPoolExecutor getThreadPoolExecutor() {
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
        numProcessors,
        numProcessors,
        3,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory("prefetch-engine"));
    threadPoolExecutor.allowCoreThreadTimeOut(true); // allow core threads to timeout
    return threadPoolExecutor;
  }

  private ReplicaPrefetchEngine() {

  }

  private static String getEngineStats() {
    return "PrefetchEngineStats - currentTaskCount: " + prefetchEngineExecutor.getTaskCount() + " completedTaskCount: "
        + prefetchEngineExecutor.getCompletedTaskCount() + " activeCount: " + prefetchEngineExecutor.getActiveCount();
  }

  public static void triggerPrefetch(RecordPrefetchContext rpc) {
    Runnable prefetchTask = () -> {
      try {
        StoreIngestionTask sit = rpc.getSit();
        if (sit == null || !sit.isRunning.get() || !sit.isPrefetchEnabled() || rpc.numKeys() == 0
            || !sit.isLeader(rpc.partitionId)) {
          LOGGER.warn(
              "Skipping prefetch for rpc: {}. sit: {} isRunning: {} isPrefetchEnabled: {} numKeys: {} isLeader: {}",
              rpc,
              sit,
              sit != null ? sit.isRunning.get() : "N/A",
              sit != null ? sit.isPrefetchEnabled() : "N/A",
              rpc.numKeys(),
              sit != null ? sit.isLeader(rpc.partitionId) : "N/A");
          return;
        }

        LOGGER.info("Starting prefetch for rpc: {}", rpc);
        rpc.sit.prefetchRecords(rpc.partitionId, rpc.getKeysToFetchList());
      } catch (Exception e) {
        LOGGER.error("Error prefetching records for {}", rpc, e);
      }
    };
    prefetchEngineExecutor.submit(prefetchTask);
    LOGGER.info("Submitted prefetch task for rpc: {}. {}", rpc, getEngineStats());
  }

  static class RecordPrefetchContext {
    private final List<KafkaKey> keysToFetch;
    private final StoreIngestionTask sit;
    private final int partitionId;

    RecordPrefetchContext(StoreIngestionTask sit, int partitionId, List<KafkaKey> keysToFetch) {
      this.partitionId = partitionId;
      this.keysToFetch = keysToFetch;
      this.sit = sit;
    }

    void addKey(KafkaKey key) {
      keysToFetch.add(key);
    }

    Iterator<KafkaKey> getKeysToFetch() {
      return keysToFetch.iterator();
    }

    List<KafkaKey> getKeysToFetchList() {
      return keysToFetch;
    }

    int numKeys() {
      return keysToFetch.size();
    }

    StoreIngestionTask getSit() {
      return sit;
    }

    @Override
    public String toString() {
      return "PrefetchContext{vtp: " + sit.getVersionTopic().getName() + "-" + partitionId + ", numKeys: " + numKeys()
          + "}";
    }
  }
}
