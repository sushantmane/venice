package com.linkedin.venice.listener;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.concurrent.atomic.AtomicInteger;


public class VeniceServerNettyStats extends AbstractVeniceStats {
  private final AtomicInteger activeConnections = new AtomicInteger();

  private final AtomicInteger activeReadHandlerThreads = new AtomicInteger();
  private final Sensor writeAndFlushTimeOkRequests;
  private final Sensor writeAndFlushTimeBadRequests;
  private final Sensor writeAndFlushCompletionTimeForDataRequest;
  private final AtomicInteger queuedTasksForReadHandler = new AtomicInteger();

  public VeniceServerNettyStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> activeConnections.get(), "active_connections"));

    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> activeReadHandlerThreads.get(), "active_read_handler_threads"));

    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> queuedTasksForReadHandler.get(), "queued_tasks_for_read_handler"));

    String writeAndFlushTimeOkRequestsSensorName = "WriteAndFlushTimeOkRequests";
    writeAndFlushTimeOkRequests = registerSensorIfAbsent(
        writeAndFlushTimeOkRequestsSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils
            .getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + writeAndFlushTimeOkRequestsSensorName));

    String writeAndFlushTimeBadRequestsSensorName = "WriteAndFlushTimeBadRequests";
    writeAndFlushTimeBadRequests = registerSensorIfAbsent(
        writeAndFlushTimeBadRequestsSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils
            .getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + writeAndFlushTimeBadRequestsSensorName));

    String responseWriteAndFlushStartTimeNanosSensorName = "WriteAndFlushCompletionTimeForDataRequest";
    writeAndFlushCompletionTimeForDataRequest = registerSensorIfAbsent(
        responseWriteAndFlushStartTimeNanosSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(
            getName() + AbstractVeniceStats.DELIMITER + responseWriteAndFlushStartTimeNanosSensorName));
  }

  public static long getElapsedTimeInMicros(long startTimeNanos) {
    return (System.nanoTime() - startTimeNanos) / 1000;
  }

  public static long getElapsedTimeInNanos(long startTimeNanos) {
    return System.nanoTime() - startTimeNanos;
  }

  public int incrementActiveReadHandlerThreads() {
    return activeReadHandlerThreads.incrementAndGet();
  }

  public int decrementActiveReadHandlerThreads() {
    return activeReadHandlerThreads.decrementAndGet();
  }

  public int incrementActiveConnections() {
    return activeConnections.incrementAndGet();
  }

  public int decrementActiveConnections() {
    return activeConnections.decrementAndGet();
  }

  public void recordWriteAndFlushTimeOkRequests(long startTimeNanos) {
    writeAndFlushTimeOkRequests.record(getElapsedTimeInMicros(startTimeNanos));
  }

  public void recordWriteAndFlushTimeBadRequests(long startTimeNanos) {
    writeAndFlushTimeBadRequests.record(getElapsedTimeInMicros(startTimeNanos));
  }

  public void recordWriteAndFlushCompletionTimeForDataRequest(long startTimeNanos) {
    writeAndFlushCompletionTimeForDataRequest.record(getElapsedTimeInMicros(startTimeNanos));
  }

  public void incrementQueuedTasksForReadHandler() {
    queuedTasksForReadHandler.incrementAndGet();
  }

  public void decrementQueuedTasksForReadHandler() {
    queuedTasksForReadHandler.decrementAndGet();
  }
}
