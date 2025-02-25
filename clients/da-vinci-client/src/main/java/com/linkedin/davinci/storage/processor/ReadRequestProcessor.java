package com.linkedin.davinci.storage.processor;

import static com.linkedin.davinci.storage.processor.ReadRequestProcessor.RequestTrigger.READ_OP;
import static com.linkedin.davinci.storage.processor.ReadRequestProcessor.RequestTrigger.WRITE_OP;

import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ReadRequestProcessor {
  private static final Logger LOGGER = LogManager.getLogger(ReadRequestProcessor.class);

  private final BlockingQueue<Runnable> taskQueue = new PriorityBlockingQueue<>();
  private final ExecutorService executorService;

  public ReadRequestProcessor() {
    this.executorService = new CustomSingleThreadPoolExecutor(taskQueue);
  }

  /**
   * Enum representing the trigger type of task.
   * <ul>
   *   <li>{@code READ_OP} - Represents a read operation.</li>
   *   <li>{@code WRITE_OP} - Represents a ingestion triggered read operation .</li>
   * </ul>
   */
  enum RequestTrigger {
    READ_OP, WRITE_OP
  }

  // Task updated to remove Comparable implementation
  static class ReadRequestTask implements Runnable {
    final ReadRequestProcessor.RequestTrigger requestTrigger;
    final long maxDelayedUntilNanoTime;
    private final Runnable operation;

    public ReadRequestTask(
        ReadRequestProcessor.RequestTrigger requestTrigger,
        long maxDelayedUntilNanoTime,
        Runnable operation) {
      this.requestTrigger = requestTrigger;
      this.maxDelayedUntilNanoTime = maxDelayedUntilNanoTime;
      this.operation = operation;
    }

    @Override
    public void run() {
      operation.run();
      // Utils.sleep(2000); // Simulating processing delay
      // operation.run();
    }
  }

  /**
   * Submits a task for execution in the processing queue.
   *
   * @param task The {@link ReadRequestTask} to be added to the execution queue.
   */
  public void addTask(ReadRequestTask task) {
    executorService.submit(task);
  }

  public void shutdown() {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  // A custom single-threaded ThreadPoolExecutor that overrides newTaskFor to enforce prioritization
  public static class CustomSingleThreadPoolExecutor extends ThreadPoolExecutor {
    public CustomSingleThreadPoolExecutor(BlockingQueue<Runnable> workQueue) {
      super(1, 1, 1000, TimeUnit.SECONDS, workQueue);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
      return new CustomFutureTask<>(runnable, value);
    }
  }

  /**
   * A {@link FutureTask} implementation that enforces task prioritization.
   *
   * @param <T> The result type of the computation.
   */
  public static class CustomFutureTask<T> extends FutureTask<T> implements Comparable<CustomFutureTask<T>> {
    private final ReadRequestTask task;

    public CustomFutureTask(Runnable task, T value) {
      super(task, value);
      this.task = (ReadRequestTask) task;
    }

    @Override
    public String toString() {
      return task.requestTrigger + " " + task.maxDelayedUntilNanoTime;
    }

    /**
     * Compares two tasks to determine execution priority.
     *
     * <p>Prioritization rules:</p>
     * <ul>
     *   <li>WRITE tasks that have exceeded their max delay are given higher priority.</li>
     *   <li>READ tasks take priority over normal WRITE tasks.</li>
     *   <li>Among READ tasks, those with an earlier `maxDelayedUntil` execute first.</li>
     * </ul>
     *
     * @param that The other task to compare against.
     * @return Comparison result defining execution order.
     */

    @Override
    public int compareTo(CustomFutureTask<T> that) {
      if (this.task.requestTrigger == READ_OP
          && this.task.maxDelayedUntilNanoTime < that.task.maxDelayedUntilNanoTime) {
        return -1;
      }

      // Both are READ: prioritize by earlier maxDelayedUntil
      if (this.task.requestTrigger == READ_OP && that.task.requestTrigger == READ_OP) {
        return Long.compare(this.task.maxDelayedUntilNanoTime, that.task.maxDelayedUntilNanoTime);
      }

      // Both are WRITE: prioritize by earlier maxDelayedUntil
      if (this.task.requestTrigger == WRITE_OP && that.task.requestTrigger == WRITE_OP) {
        return Long.compare(this.task.maxDelayedUntilNanoTime, that.task.maxDelayedUntilNanoTime);
      }

      long currentTime = instanceOfTime + 10;

      boolean thisWriteElevated =
          this.task.requestTrigger == WRITE_OP && currentTime >= this.task.maxDelayedUntilNanoTime;
      boolean otherWriteElevated =
          that.task.requestTrigger == WRITE_OP && currentTime >= that.task.maxDelayedUntilNanoTime;

      if (thisWriteElevated && !otherWriteElevated) {
        return -1; // This WRITE has been elevated to high priority
      } else if (!thisWriteElevated && otherWriteElevated) {
        return 1; // Other WRITE has been elevated, so it runs first
      }

      // READ vs WRITE: READ has priority unless WRITE is elevated
      if (this.task.requestTrigger == READ_OP && that.task.requestTrigger == WRITE_OP) {
        return -1; // READ takes priority over non-elevated WRITE
      } else if (this.task.requestTrigger == WRITE_OP && that.task.requestTrigger == READ_OP) {
        return 1; // WRITE has lower priority unless elevated
      }

      return 0; // Otherwise, equal priority
    }

    public int compareTo1(CustomFutureTask<T> that) {
      // Both are READ: prioritize by earlier maxDelayedUntil
      if (this.task.requestTrigger == READ_OP && that.task.requestTrigger == READ_OP) {
        return Long.compare(this.task.maxDelayedUntilNanoTime, that.task.maxDelayedUntilNanoTime);
      }
      // Both are WRITE: prioritize by earlier maxDelayedUntil
      if (this.task.requestTrigger == WRITE_OP && that.task.requestTrigger == WRITE_OP) {
        return Long.compare(this.task.maxDelayedUntilNanoTime, that.task.maxDelayedUntilNanoTime);
      }
      if (this.task.requestTrigger == READ_OP && that.task.requestTrigger == WRITE_OP) {
        return compareReadAndWrite(this, that);
      }
      if (this.task.requestTrigger == WRITE_OP && that.task.requestTrigger == READ_OP) {
        return -compareReadAndWrite(that, this);
      }
      return 0;
    }

    private int compareReadAndWrite(CustomFutureTask<T> readFutureTask, CustomFutureTask<T> writeFutureTask) {
      ReadRequestTask readTask = readFutureTask.task;
      ReadRequestTask writeTask = writeFutureTask.task;
      if (readTask.requestTrigger != READ_OP || writeTask.requestTrigger != WRITE_OP) {
        throw new IllegalArgumentException(
            "First task must be a READ task and second task must be a WRITE task but got: " + readTask.requestTrigger
                + " and " + writeTask.requestTrigger + " respectively");
      }
      long currentTime = instanceOfTime + 10;
      // READ task takes priority (negative result) if:
      // 1. both readTask and writeTask have larger than currentTime delay
      // 2. both readTask and writeTask have smaller than currentTime delay
      if (readTask.maxDelayedUntilNanoTime >= currentTime && writeTask.maxDelayedUntilNanoTime >= currentTime) {
        return -1;
      }
      if (readTask.maxDelayedUntilNanoTime <= currentTime && writeTask.maxDelayedUntilNanoTime <= currentTime) {
        return -1;
      }
      if (writeTask.maxDelayedUntilNanoTime >= currentTime) {
        return 1;
      }

      return -1;
    }
  }

  public static final long instanceOfTime = 0;

  public static void main(String[] args) {
    long startTimeNs = instanceOfTime;

    BlockingQueue<CustomFutureTask> taskQueue = new PriorityBlockingQueue<>();
    for (int i = 0; i < 20; i += 1) {
      int finalI = i;
      long nanos = finalI + startTimeNs;

      taskQueue.add(
          new CustomFutureTask(
              new ReadRequestTask(READ_OP, nanos, () -> System.out.println("Read Task: " + nanos)),
              null));
      taskQueue.add(
          new CustomFutureTask(
              new ReadRequestTask(WRITE_OP, nanos, () -> System.out.println("Write Task: " + nanos)),
              null));
    }

    List<CustomFutureTask> tasks = new ArrayList<>(taskQueue.size());

    while (!taskQueue.isEmpty()) {
      CustomFutureTask task = taskQueue.poll();
      if (task != null) {
        tasks.add(task);
      }
      task.run();
    }
    if (taskQueue.isEmpty()) {
      System.out.println(tasks);
      return;
    }

    ReadRequestProcessor processor = new ReadRequestProcessor();

    // Adding tasks
    processor.addTask(new ReadRequestTask(WRITE_OP, System.nanoTime(), () -> {
      System.out.println("------BLOCK------");
      Utils.sleep(1000);
    }));
    Utils.sleep(1000);
    long now = System.nanoTime();
    processor.addTask(new ReadRequestTask(WRITE_OP, now + 10, () -> System.out.println("Write Task 1 executed")));
    processor.addTask(new ReadRequestTask(READ_OP, now + 5, () -> System.out.println("Read Task 1 executed")));
    processor.addTask(new ReadRequestTask(READ_OP, now + 2, () -> System.out.println("Read Task 2 executed")));
    processor.addTask(new ReadRequestTask(WRITE_OP, now + 1, () -> System.out.println("Write Task 2 executed")));
    processor.addTask(new ReadRequestTask(WRITE_OP, now + 5, () -> System.out.println("Write Task 3 executed")));
    processor.addTask(new ReadRequestTask(WRITE_OP, now, () -> System.out.println("Write Task Elevated executed")));

    Utils.sleep(11000);

    Utils.sleep(60_000);

    processor.shutdown();
  }
}
