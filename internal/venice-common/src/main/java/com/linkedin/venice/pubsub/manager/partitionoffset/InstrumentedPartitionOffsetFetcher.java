// package com.linkedin.venice.pubsub.manager.partitionoffset;
//
// import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
// import com.linkedin.venice.stats.AbstractVeniceStats;
// import com.linkedin.venice.stats.TehutiUtils;
// import com.linkedin.venice.utils.Time;
// import com.linkedin.venice.utils.Utils;
// import io.tehuti.metrics.MetricsRepository;
// import io.tehuti.metrics.Sensor;
// import io.tehuti.metrics.stats.Avg;
// import io.tehuti.metrics.stats.Max;
// import io.tehuti.metrics.stats.Min;
// import io.tehuti.metrics.stats.OccurrenceRate;
// import java.util.EnumMap;
// import javax.annotation.Nonnull;
// import org.apache.commons.lang.Validate;
//
//
// public class InstrumentedPartitionOffsetFetcher implements PartitionOffsetFetcher {
// private final PartitionOffsetFetcher partitionOffsetFetcher;
// private final PartitionOffsetFetcherStats stats;
// private final Time time;
//
// public InstrumentedPartitionOffsetFetcher(
// @Nonnull PartitionOffsetFetcher partitionOffsetFetcher,
// @Nonnull PartitionOffsetFetcherStats stats,
// @Nonnull Time time) {
// Validate.notNull(partitionOffsetFetcher);
// Validate.notNull(stats);
// Validate.notNull(time);
// this.partitionOffsetFetcher = partitionOffsetFetcher;
// this.stats = stats;
// this.time = time;
// }
//
// // @Override
// // public Int2LongMap getTopicLatestOffsets(PubSubTopic topic) {
// // final long startTimeMs = time.getMilliseconds();
// // Int2LongMap res = partitionOffsetFetcher.getTopicLatestOffsets(topic);
// // stats.recordLatency(
// // PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_LATEST_OFFSETS,
// // Utils.calculateDurationMs(time, startTimeMs));
// // return res;
// // }
// //
// // @Override
// // public long getPartitionLatestOffsetAndRetry(PubSubTopicPartition topicPartition, int retries) {
// // final long startTimeMs = time.getMilliseconds();
// // long res = partitionOffsetFetcher.getPartitionLatestOffsetAndRetry(topicPartition, retries);
// // stats.recordLatency(
// // PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_LATEST_OFFSET_WITH_RETRY,
// // Utils.calculateDurationMs(time, startTimeMs));
// // return res;
// // }
// //
// // @Override
// // public long getPartitionEarliestOffsetAndRetry(PubSubTopicPartition topicPartition, int retries) {
// // final long startTimeMs = time.getMilliseconds();
// // long res = partitionOffsetFetcher.getPartitionEarliestOffsetAndRetry(topicPartition, retries);
// // stats.recordLatency(
// // PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_EARLIEST_OFFSET_WITH_RETRY,
// // Utils.calculateDurationMs(time, startTimeMs));
// // return res;
// // }
//
// @Override
// public long getPartitionOffsetByTime(PubSubTopicPartition topicPartition, long timestamp) {
// final long startTimeMs = time.getMilliseconds();
// long res = partitionOffsetFetcher.getPartitionOffsetByTime(topicPartition, timestamp);
// stats.recordLatency(
// PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_OFFSET_BY_TIME,
// Utils.calculateDurationMs(time, startTimeMs));
// return res;
// }
//
// @Override
// public long getProducerTimestampOfLastDataRecord(PubSubTopicPartition topicPartition, int retries) {
// final long startTimeMs = time.getMilliseconds();
// long res = partitionOffsetFetcher.getProducerTimestampOfLastDataRecord(topicPartition, retries);
// stats.recordLatency(
// PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_LATEST_PRODUCER_TIMESTAMP_ON_DATA_RECORD_WITH_RETRY,
// Utils.calculateDurationMs(time, startTimeMs));
// return res;
// }
//
// // @Override
// // public List<PubSubTopicPartitionInfo> getTopicPartitionInfo(PubSubTopic topic) {
// // final long startTimeMs = time.getMilliseconds();
// // List<PubSubTopicPartitionInfo> res = partitionOffsetFetcher.getTopicPartitionInfo(topic);
// // stats.recordLatency(
// // PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.PARTITIONS_FOR,
// // Utils.calculateDurationMs(time, startTimeMs));
// // return res;
// // }
//
// @Override
// public long getOffsetByTimeIfOutOfRange(PubSubTopicPartition topicPartition, long timestamp) {
// final long startTimeMs = time.getMilliseconds();
// long res = partitionOffsetFetcher.getOffsetByTimeIfOutOfRange(topicPartition, timestamp);
// stats.recordLatency(
// PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_OFFSET_BY_TIME_IF_OUT_OF_RANGE,
// Utils.calculateDurationMs(time, startTimeMs));
// return res;
// }
//
// @Override
// public void close() {
// partitionOffsetFetcher.close();
// }
//
// /**
// * Stats for {@link PartitionOffsetFetcher}
// */
// public static class PartitionOffsetFetcherStats extends AbstractVeniceStats {
// public enum OCCURRENCE_LATENCY_SENSOR_TYPE {
// GET_TOPIC_LATEST_OFFSETS, GET_PARTITION_LATEST_OFFSET_WITH_RETRY, GET_PARTITIONS_OFFSETS_BY_TIME,
// GET_PARTITION_OFFSET_BY_TIME, GET_LATEST_PRODUCER_TIMESTAMP_ON_DATA_RECORD_WITH_RETRY, PARTITIONS_FOR,
// GET_PARTITION_OFFSET_BY_TIME_IF_OUT_OF_RANGE, GET_PARTITION_EARLIEST_OFFSET_WITH_RETRY
// }
//
// private final EnumMap<OCCURRENCE_LATENCY_SENSOR_TYPE, Sensor> sensorsByTypes;
//
// public PartitionOffsetFetcherStats(MetricsRepository metricsRepository, String name) {
// super(metricsRepository, name);
// this.sensorsByTypes = new EnumMap<>(OCCURRENCE_LATENCY_SENSOR_TYPE.class);
// for (OCCURRENCE_LATENCY_SENSOR_TYPE sensorType: OCCURRENCE_LATENCY_SENSOR_TYPE.values()) {
// final String sensorName = sensorType.name().toLowerCase();
// sensorsByTypes.put(
// sensorType,
// registerSensorIfAbsent(
// sensorName,
// new OccurrenceRate(),
// new Max(),
// new Min(),
// new Avg(),
// TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + sensorName)));
// }
// }
//
// public void recordLatency(OCCURRENCE_LATENCY_SENSOR_TYPE sensor_type, long requestLatencyMs) {
// sensorsByTypes.get(sensor_type).record(requestLatencyMs);
// }
// }
// }
