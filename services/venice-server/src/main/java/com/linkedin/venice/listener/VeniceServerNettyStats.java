package com.linkedin.venice.listener;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.netty.util.AttributeKey;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import java.util.concurrent.atomic.AtomicInteger;


public class VeniceServerNettyStats extends AbstractVeniceStats {
  public static final AttributeKey<Long> FIRST_HANDLER_TIMESTAMP_KEY = AttributeKey.valueOf("FirstHandlerTimestamp");

  private final AtomicInteger activeConnections = new AtomicInteger();

  private final AtomicInteger activeReadHandlerThreads = new AtomicInteger();
  private final Sensor timeSpentTillHandoffToReadHandler;
  private final Sensor timeSpentInQuotaEnforcement;
  private final Sensor nettyFlushCounter;

  private final Sensor storageExecutionHandlerSubmissionWaitTime;
  private final Sensor nonOkResponseLatency;
  private final Sensor requestArrivalRate;
  private final Sensor requestProcessingRate;

  private final Sensor ioRequestArrivalRate;
  private final Sensor ioRequestProcessingRate;

  public VeniceServerNettyStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> activeConnections.get(), "active_connections"));

    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> activeReadHandlerThreads.get(), "active_read_handler_threads"));

    nettyFlushCounter = registerSensor("nettyFlushCounter", new Rate(), new Avg(), new Max());

    String timeSpentTillHandoffToReadHandlerSensorName = "TimeSpentTillHandoffToReadHandler";
    timeSpentTillHandoffToReadHandler = registerSensorIfAbsent(
        timeSpentTillHandoffToReadHandlerSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(
            getName() + AbstractVeniceStats.DELIMITER + timeSpentTillHandoffToReadHandlerSensorName));

    String timeSpentInQuotaEnforcementSensorName = "TimeSpentInQuotaEnforcement";
    timeSpentInQuotaEnforcement = registerSensorIfAbsent(
        timeSpentInQuotaEnforcementSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils
            .getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + timeSpentInQuotaEnforcementSensorName));

    String storageExecutionHandlerSubmissionWaitTimeSensorName = "storage_execution_handler_submission_wait_time";

    storageExecutionHandlerSubmissionWaitTime = registerSensorIfAbsent(
        storageExecutionHandlerSubmissionWaitTimeSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(
            getName() + AbstractVeniceStats.DELIMITER + storageExecutionHandlerSubmissionWaitTimeSensorName));

    String nonOkResponseLatencySensorName = "non_ok_response_latency";
    nonOkResponseLatency = registerSensorIfAbsent(
        nonOkResponseLatencySensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + nonOkResponseLatencySensorName));

    String requestArrivalRateSensorName = "request_arrival_rate";

    requestArrivalRate =
        registerSensorIfAbsent(requestArrivalRateSensorName, new OccurrenceRate(), new Avg(), new Max());

    String requestProcessingRateSensorName = "request_processing_rate";
    requestProcessingRate =
        registerSensorIfAbsent(requestProcessingRateSensorName, new OccurrenceRate(), new Avg(), new Max());

    String ioRequestArrivalRateSensorName = "io_request_arrival_rate";
    ioRequestArrivalRate =
        registerSensorIfAbsent(ioRequestArrivalRateSensorName, new OccurrenceRate(), new Avg(), new Max());

    String ioRequestProcessingRateSensorName = "io_request_processing_rate";
    ioRequestProcessingRate = registerSensorIfAbsent(
        ioRequestProcessingRateSensorName,
        new OccurrenceRate(),
        new Max(),
        new Min(),
        new Avg(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + ioRequestProcessingRateSensorName));
  }

  private static final double NANO_TO_MILLIS = 1_000_000;

  public static double getElapsedTimeInMillis(long startTimeNanos) {
    return (System.nanoTime() - startTimeNanos) / NANO_TO_MILLIS;
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

  // get activeConnections
  public int getActiveConnections() {
    return activeConnections.get();
  }

  public int decrementActiveConnections() {
    return activeConnections.decrementAndGet();
  }

  public void recordTimeSpentTillHandoffToReadHandler(long startTimeNanos) {
    timeSpentTillHandoffToReadHandler.record(getElapsedTimeInMillis(startTimeNanos));
  }

  public void recordTimeSpentInQuotaEnforcement(long startTimeNanos) {
    timeSpentInQuotaEnforcement.record(getElapsedTimeInMillis(startTimeNanos));
  }

  public void recordNettyFlushCounts() {
    nettyFlushCounter.record(1);
  }

  public void recordStorageExecutionHandlerSubmissionWaitTime(double submissionWaitTime) {
    storageExecutionHandlerSubmissionWaitTime.record(submissionWaitTime);
  }

  public void recordNonOkResponseLatency(double latency) {
    nonOkResponseLatency.record(latency);
  }

  public void recordRequestArrivalRate() {
    requestArrivalRate.record();
  }

  public void recordRequestProcessingRate() {
    requestProcessingRate.record();
  }

  public void recordIoRequestArrivalRate() {
    ioRequestArrivalRate.record();
  }

  public void recordIoRequestProcessingRate(double elapsedTime) {
    ioRequestProcessingRate.record(elapsedTime);
  }
}
