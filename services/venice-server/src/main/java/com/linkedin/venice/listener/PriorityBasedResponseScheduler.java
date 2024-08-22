package com.linkedin.venice.listener;

import com.linkedin.venice.utils.DaemonThreadFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PriorityBasedResponseScheduler {
  private static final Logger LOGGER = LogManager.getLogger(PriorityBasedResponseScheduler.class);

  private final Map<EventLoop, LinkedBlockingQueue<ResponseEvent>> responseEventMap;
  private final ThreadPoolExecutor executor;
  private static final long HOLD_THRESHOLD = Duration.ofMillis(5).toNanos();

  public PriorityBasedResponseScheduler(PriorityBasedResponseSchedulerContext context) {
    responseEventMap = new ConcurrentHashMap<>(context.numQueues);
    executor = createThreadPool(context.numThreads);
    executor.execute(() -> {
      while (true) {
        long sleepTime = 10_000_000; // 10 ms
        for (Map.Entry<EventLoop, LinkedBlockingQueue<ResponseEvent>> entry: responseEventMap.entrySet()) {
          LinkedBlockingQueue<ResponseEvent> responseEventQueue = entry.getValue();
          ResponseEvent responseEvent = responseEventQueue.peek();
          if (responseEvent == null) {
            continue;
          }
          long evtArrivalTime = responseEvent.timestampInNanos;
          long remainingTime = evtArrivalTime + HOLD_THRESHOLD - System.nanoTime();
          sleepTime = Math.max(0, Math.min(sleepTime, remainingTime));
          // check if at least 10 ms have passed since the event arrived
          if (System.nanoTime() - evtArrivalTime < HOLD_THRESHOLD) {
            continue;
          }

          responseEvent = responseEventQueue.poll();
          if (responseEvent == null) {
            continue;
          }
          ResponseEvent finalResponseEvent = responseEvent;
          executor.execute(() -> {
            try {
              finalResponseEvent.context.writeAndFlush(finalResponseEvent.message);
            } catch (Exception e) {
              LOGGER.error("Failed to write and flush response", e);
            }
          });
        }

        // sleep for sleepTime nanos
        try {
          Thread.sleep(sleepTime / 1_000_000, (int) (sleepTime % 1_000_000));
        } catch (InterruptedException e) {
          LOGGER.error("Interrupted while sleeping", e);
        }
      }
    });
  }

  public void writeAndFlush(ChannelHandlerContext context, Object message) {
    EventLoop currentEventLoop = context.channel().eventLoop();
    LinkedBlockingQueue<ResponseEvent> responseEventQueue = responseEventMap.get(currentEventLoop);
    if (responseEventQueue == null) {
      responseEventQueue = new LinkedBlockingQueue<>();
      responseEventMap.put(currentEventLoop, responseEventQueue);
    }
    ResponseEvent responseEvent = new ResponseEvent();
    responseEvent.context = context;
    responseEvent.message = message;
    responseEvent.timestampInNanos = System.nanoTime();
    responseEventQueue.add(responseEvent);
  }

  public static class ResponseEvent {
    public ChannelHandlerContext context;
    public Object message;
    public long timestampInNanos;
  }

  private ThreadPoolExecutor createThreadPool(int threadCount) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        threadCount,
        threadCount,
        5,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory("ResponseScheduler"));
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    executor.prestartCoreThread();
    return executor;
  }
}
