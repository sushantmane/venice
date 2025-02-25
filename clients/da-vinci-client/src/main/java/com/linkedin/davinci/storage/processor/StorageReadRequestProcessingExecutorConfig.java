package com.linkedin.davinci.storage.processor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


public class StorageReadRequestProcessingExecutorConfig {
  private final int corePoolSize;
  private final int maxPoolSize;
  private final long keepAliveTime;
  private final TimeUnit timeUnit;
  private final BlockingQueue<Runnable> taskQueue;
  private final ThreadFactory threadFactory;
  private final RejectedExecutionHandler rejectedExecutionHandler;
  private final boolean isSharedStorageReadPoolWithPrioritization;

  private StorageReadRequestProcessingExecutorConfig(Builder builder) {
    this.corePoolSize = builder.corePoolSize;
    this.maxPoolSize = builder.maxPoolSize;
    this.keepAliveTime = builder.keepAliveTime;
    this.timeUnit = builder.timeUnit;
    this.taskQueue = builder.taskQueue;
    this.threadFactory = builder.threadFactory;
    this.rejectedExecutionHandler = builder.rejectedExecutionHandler;
    this.isSharedStorageReadPoolWithPrioritization = builder.isSharedStorageReadPoolWithPrioritization;
  }

  public int getCorePoolSize() {
    return corePoolSize;
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public long getKeepAliveTime() {
    return keepAliveTime;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public BlockingQueue<Runnable> getTaskQueue() {
    return taskQueue;
  }

  public ThreadFactory getThreadFactory() {
    return threadFactory;
  }

  public RejectedExecutionHandler getRejectedExecutionHandler() {
    return rejectedExecutionHandler;
  }

  public boolean isSharedStorageReadPoolWithPrioritization() {
    return isSharedStorageReadPoolWithPrioritization;
  }

  public static class Builder {
    private int corePoolSize;
    private int maxPoolSize;
    private long keepAliveTime;
    private TimeUnit timeUnit;
    private BlockingQueue<Runnable> taskQueue;
    private ThreadFactory threadFactory;
    private RejectedExecutionHandler rejectedExecutionHandler;
    private boolean isSharedStorageReadPoolWithPrioritization;

    public Builder setCorePoolSize(int corePoolSize) {
      this.corePoolSize = corePoolSize;
      return this;
    }

    public Builder setMaxPoolSize(int maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
      return this;
    }

    public Builder setKeepAliveTime(long keepAliveTime) {
      this.keepAliveTime = keepAliveTime;
      return this;
    }

    public Builder setTimeUnit(TimeUnit timeUnit) {
      this.timeUnit = timeUnit;
      return this;
    }

    public Builder setTaskQueue(BlockingQueue<Runnable> taskQueue) {
      this.taskQueue = taskQueue;
      return this;
    }

    public Builder setThreadFactory(ThreadFactory threadFactory) {
      this.threadFactory = threadFactory;
      return this;
    }

    public Builder setRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
      this.rejectedExecutionHandler = rejectedExecutionHandler;
      return this;
    }

    public StorageReadRequestProcessingExecutorConfig build() {
      if (corePoolSize <= 0) {
        throw new IllegalArgumentException("Core pool size must be greater than 0");
      }
      if (maxPoolSize <= 0) {
        throw new IllegalArgumentException("Max pool size must be greater than 0");
      }
      if (keepAliveTime <= 0) {
        throw new IllegalArgumentException("Keep alive time must be greater than 0");
      }
      if (timeUnit == null) {
        throw new IllegalArgumentException("Time unit must not be null");
      }
      if (taskQueue == null) {
        throw new IllegalArgumentException("Task queue must not be null");
      }
      if (threadFactory == null) {
        throw new IllegalArgumentException("Thread factory must not be null");
      }
      if (rejectedExecutionHandler == null) {
        throw new IllegalArgumentException("Rejected execution handler must not be null");
      }
      return new StorageReadRequestProcessingExecutorConfig(this);
    }
  }
}
