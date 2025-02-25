package com.linkedin.davinci.storage.processor;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import javax.annotation.Nonnull;


public class StorageReadRequestProcessingExecutor extends ThreadPoolExecutor implements Executor, AutoCloseable {
  private final boolean isSharedStorageReadPoolWithPrioritization = false;
  private final ThreadPoolExecutor threadPoolExecutor;

  public StorageReadRequestProcessingExecutor(StorageReadRequestProcessingExecutorConfig config) {
    super(
        config.getCorePoolSize(),
        config.getMaxPoolSize(),
        config.getKeepAliveTime(),
        config.getTimeUnit(),
        config.getTaskQueue(),
        config.getThreadFactory(),
        config.getRejectedExecutionHandler());
    if (config.isSharedStorageReadPoolWithPrioritization()) {
      throw new IllegalArgumentException("Shared storage read pool with prioritization is not supported");
    } else {
      this.threadPoolExecutor = new ThreadPoolExecutor(
          config.getCorePoolSize(),
          config.getMaxPoolSize(),
          config.getKeepAliveTime(),
          config.getTimeUnit(),
          config.getTaskQueue(),
          config.getThreadFactory(),
          config.getRejectedExecutionHandler());
    }
  }

  public Future<?> submit(Runnable task) {
    return threadPoolExecutor.submit(task);
  }

  public int getQueueLength() {
    return threadPoolExecutor.getQueue().size();
  }

  @Override
  public void execute(@Nonnull Runnable command) {
    threadPoolExecutor.execute(command);
  }

  public void executeUserRead(@Nonnull Runnable command) {
    if (!isSharedStorageReadPoolWithPrioritization) {
      threadPoolExecutor.execute(command);
      return;
    }

  }

  @Override
  public void close() throws Exception {
    threadPoolExecutor.shutdown();
  }
}
