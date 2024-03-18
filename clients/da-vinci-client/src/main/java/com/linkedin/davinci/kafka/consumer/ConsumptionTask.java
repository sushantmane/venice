package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.ReplicaPrefetchEngine.*;

import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.stats.Rate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This {@link Runnable} is a loop encapsulating the minimal amount of state and function handles in order to pipe
 * consumed messages into their intended {@link ConsumedDataReceiver} instances.
 *
 * It can poll messages but without knowing the exact polling strategy, nor can it do anything else directly to mutate
 * the state of the consumer. It can do so indirectly however by interacting with a {@link ConsumerSubscriptionCleaner}.
 * In the future, we may want to consider adding more subscription APIs into this class such that it could become the
 * only point of entry into a given consumer instance, which could allow us to ditch the {@link SharedKafkaConsumer}
 * and to safely use an unsynchronized consumer instead. For now, there are still multiple code paths which can end up
 * affecting the consumer used by this task, so it is still necessary to use a threadsafe one.
 *
 * Besides polling, on each iteration of the {@link #run()} loop, the following responsibilities are also fulfilled:
 * 1. Invoking the functions in {@link ConsumerSubscriptionCleaner} to ensure the consumer's subscriptions are valid.
 * 2. Invoking two throttlers, for bandwidth and records throughput.
 * 3. Recording some stats.
 */
class ConsumptionTask implements Runnable {
  private final Logger LOGGER;
  private final int taskId;
  private final Map<PubSubTopicPartition, ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>>> dataReceiverMap =
      new VeniceConcurrentHashMap<>();
  private final long readCycleDelayMs;
  private final Supplier<Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>>> pollFunction;
  private final IntConsumer bandwidthThrottler;
  private final IntConsumer recordsThrottler;
  private final AggKafkaConsumerServiceStats aggStats;
  private final ConsumerSubscriptionCleaner cleaner;

  /**
   * Maintain rate counter with default window size to calculate the message and bytes rate at topic partition level.
   * Those topic partition level information will not be emitted out as a metric, to avoid emitting too many metrics per
   * server host, they are for admin tool debugging purpose.
   */
  private final Map<PubSubTopicPartition, Rate> messageRatePerTopicPartition = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Rate> bytesRatePerTopicPartition = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Long> lastSuccessfulPollTimestampPerTopicPartition =
      new VeniceConcurrentHashMap<>();

  private final MetricConfig metricConfig = new MetricConfig();

  private volatile boolean running = true;

  /**
   * Timestamp of the last poll. Initialized at construction time, in case the consumer task thread gets stuck from
   * the get-go.
   */
  private volatile long lastSuccessfulPollTimestamp = System.currentTimeMillis();

  /**
   * If a topic partition has not got any record polled back, we use -1 for the last poll timestamp.
   */
  public final static long DEFAULT_TOPIC_PARTITION_NO_POLL_TIMESTAMP = -1L;

  private final PriorityQueue<PubSubTopicPartition> priorityQueue = new PriorityQueue<>(getPqComparator());

  public ConsumptionTask(
      final String kafkaUrl,
      final int taskId,
      final long readCycleDelayMs,
      final Supplier<Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>>> pollFunction,
      final IntConsumer bandwidthThrottler,
      final IntConsumer recordsThrottler,
      final AggKafkaConsumerServiceStats aggStats,
      final ConsumerSubscriptionCleaner cleaner) {
    this.taskId = taskId;
    this.readCycleDelayMs = readCycleDelayMs;
    this.pollFunction = pollFunction;
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.aggStats = aggStats;
    this.cleaner = cleaner;
    String kafkaUrlForLogger = Utils.getSanitizedStringForLogger(kafkaUrl);
    this.LOGGER = LogManager.getLogger(getClass().getSimpleName() + "--" + kafkaUrlForLogger + "--t" + taskId);
  }

  @Override
  public void run() {
    boolean addSomeDelay = false;

    // Pre-allocate some variables to clobber in the loop
    long beforePollingTimeStamp;
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> polledPubSubMessages;
    long beforeProducingToWriteBufferTimestamp;
    ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumedDataReceiver;
    Set<PubSubTopicPartition> topicPartitionsToUnsub = new HashSet<>();
    int payloadBytesConsumedInOnePoll;
    int polledPubSubMessagesCount = 0;
    Map<String, StorePollCounter> storePollCounterMap = new HashMap<>();
    try {
      while (running) {
        try {
          if (addSomeDelay) {
            synchronized (this) {
              /**
               * N.B. Using {@link #wait(long)} here so that it can be interrupted by the notification of {@link #stop()}
               * or {@link #setDataReceiver(TopicPartition, ConsumedDataReceiver)}.
               */
              wait(readCycleDelayMs);
            }
            addSomeDelay = false;
          }

          beforePollingTimeStamp = System.currentTimeMillis();
          topicPartitionsToUnsub = cleaner.getTopicPartitionsToUnsubscribe(topicPartitionsToUnsub); // N.B. cheap call

          for (PubSubTopicPartition topicPartitionToUnSub: topicPartitionsToUnsub) {
            ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> dataReceiver =
                dataReceiverMap.remove(topicPartitionToUnSub);
            if (dataReceiver != null) {
              dataReceiver.notifyOfTopicDeletion(topicPartitionToUnSub.getPubSubTopic().getName());
            }
          }
          topicPartitionsToUnsub.clear();

          /**
           * N.B. The poll function could be synchronized here if implementing the idea presented in the top of class
           * JavaDoc, about how this class could become the sole entry point for all consumer-related interactions,
           * and thus be capable of operating on a non-threadsafe consumer.
           */
          polledPubSubMessages = pollFunction.get();
          lastSuccessfulPollTimestamp = System.currentTimeMillis();
          aggStats.recordTotalPollRequestLatency(lastSuccessfulPollTimestamp - beforePollingTimeStamp);

          if (polledPubSubMessages.isEmpty()) {
            addSomeDelay = true;
            continue;
          }

          priorityQueue.clear();
          int numTopicPartitions = polledPubSubMessages.size();
          List<PubSubTopicPartition> priorityOrderedTps = new ArrayList<>(numTopicPartitions);
          Map<PubSubTopicPartition, RecordPrefetchContext> rpcMap = new HashMap<>(16);

          for (PubSubTopicPartition tp: polledPubSubMessages.keySet()) {
            // check if we need prefetch
            RecordPrefetchContext prefetchContext = getPrefetchContext(tp, polledPubSubMessages.get(tp));
            // if we don't need to prefetch, add to priorityOrderedTopicPartitions
            if (prefetchContext == EMPTY_RPC) {
              priorityOrderedTps.add(tp);
              continue;
            }
            priorityQueue.add(tp);
            rpcMap.put(tp, prefetchContext);
          }
          List<RecordPrefetchContext> priorityOrderedRpc = new ArrayList<>(priorityQueue.size());
          // add the rest of the topic partitions
          while (!priorityQueue.isEmpty()) {
            PubSubTopicPartition tp = priorityQueue.poll();
            priorityOrderedTps.add(tp);
            RecordPrefetchContext rpc = rpcMap.get(tp);
            priorityOrderedRpc.add(rpc);
            ReplicaPrefetchEngine.triggerPrefetch(rpc);
          }

          // log who's determined to be prefetched
          if (!priorityOrderedRpc.isEmpty()) {
            LOGGER.info("Topic partitions determined to be prefetched: {}", priorityOrderedRpc);
          }

          payloadBytesConsumedInOnePoll = 0;
          polledPubSubMessagesCount = 0;
          beforeProducingToWriteBufferTimestamp = System.currentTimeMillis();

          for (PubSubTopicPartition curTp: priorityOrderedTps) {
            List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> topicPartitionMessages =
                polledPubSubMessages.get(curTp);

            String storeName = Version.parseStoreFromKafkaTopicName(curTp.getTopicName());
            StorePollCounter counter = storePollCounterMap.computeIfAbsent(storeName, k -> new StorePollCounter(0, 0));

            consumedDataReceiver = dataReceiverMap.get(curTp);
            if (consumedDataReceiver == null) {
              // defensive code
              LOGGER.error(
                  "Couldn't find consumed data receiver for topic partition : {} after receiving records from `poll` request",
                  curTp);
              topicPartitionsToUnsub.add(curTp);
              continue;
            }
            polledPubSubMessagesCount += topicPartitionMessages.size();
            counter.msgCount += topicPartitionMessages.size();
            int payloadSizePerTopicPartition = 0;
            for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage: topicPartitionMessages) {
              payloadSizePerTopicPartition += pubSubMessage.getPayloadSize();
            }
            counter.byteSize += payloadSizePerTopicPartition;
            payloadBytesConsumedInOnePoll += payloadSizePerTopicPartition;

            lastSuccessfulPollTimestampPerTopicPartition.put(curTp, lastSuccessfulPollTimestamp);
            messageRatePerTopicPartition.computeIfAbsent(curTp, tp -> createRate(lastSuccessfulPollTimestamp))
                .record(topicPartitionMessages.size(), lastSuccessfulPollTimestamp);
            bytesRatePerTopicPartition.computeIfAbsent(curTp, tp -> createRate(lastSuccessfulPollTimestamp))
                .record(payloadSizePerTopicPartition, lastSuccessfulPollTimestamp);

            consumedDataReceiver.write(topicPartitionMessages);
          }
          aggStats.recordTotalConsumerRecordsProducingToWriterBufferLatency(
              LatencyUtils.getElapsedTimeInMs(beforeProducingToWriteBufferTimestamp));
          aggStats.recordTotalNonZeroPollResultNum(polledPubSubMessagesCount);
          storePollCounterMap.forEach((storeName, counter) -> {
            aggStats.getStoreStats(storeName).recordPollResultNum(counter.msgCount);
            aggStats.getStoreStats(storeName).recordByteSizePerPoll(counter.byteSize);
          });
          bandwidthThrottler.accept(payloadBytesConsumedInOnePoll);
          recordsThrottler.accept(polledPubSubMessagesCount);
          cleaner.unsubscribe(topicPartitionsToUnsub);
          aggStats.recordTotalDetectedNoRunningIngestionTopicPartitionNum(topicPartitionsToUnsub.size());
          storePollCounterMap.clear();
        } catch (Exception e) {
          if (ExceptionUtils.recursiveClassEquals(e, InterruptedException.class)) {
            // We sometimes wrap InterruptedExceptions, so not taking any chances...
            LOGGER.error("Received InterruptedException, will exit");
            break;
          }
          LOGGER.error("Received exception while polling, will retry", e);
          addSomeDelay = true;
          aggStats.recordTotalPollError();
        }
      }
    } catch (Throwable t) {
      // This is a catch-all to ensure that the thread doesn't die unexpectedly. If it does, we want to know about it.
      LOGGER.error(
          "Shared consumer thread: {} exited due to an unexpected exception",
          Thread.currentThread().getName(),
          t);
    } finally {
      LOGGER.info("Shared consumer thread: {} exited", Thread.currentThread().getName());
    }
  }

  private RecordPrefetchContext getPrefetchContext(
      PubSubTopicPartition tp,
      List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> messages) {
    ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> receiver = dataReceiverMap.get(tp);
    if (!tp.isRealTime()) {
      LOGGER.debug("Not prefetching for non-realtime topic partition: {}", tp);
      return EMPTY_RPC;
    }
    if (receiver == null) {
      LOGGER.debug("Received records from `poll` request for topic partition : {} but no data receiver found", tp);
      return EMPTY_RPC;
    }
    int po = receiver.getProcessingPriority();
    if (po != ProcessingPriority.LEADER_WC_AA.getPriorityValue()
        && po != ProcessingPriority.LEADER_NO_WC_AA.getPriorityValue()) {
      return EMPTY_RPC;
    }

    List<KafkaKey> keysToFetch = new ArrayList<>();
    for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message: messages) {
      if (message.getKey().isControlMessage()) {
        continue;
      }
      keysToFetch.add(message.getKey());
    }
    if (keysToFetch.isEmpty()) {
      return EMPTY_RPC;
    }
    return new RecordPrefetchContext(receiver.getStoreIngestionTask(), tp.getPartitionNumber(), keysToFetch);
  }

  void stop() {
    running = false;
    synchronized (this) {
      notifyAll();
    }
  }

  long getLastSuccessfulPollTimestamp() {
    return lastSuccessfulPollTimestamp;
  }

  int getTaskId() {
    return taskId;
  }

  void setDataReceiver(
      PubSubTopicPartition pubSubTopicPartition,
      ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumedDataReceiver) {
    ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> previousConsumedDataReceiver =
        dataReceiverMap.put(pubSubTopicPartition, consumedDataReceiver);
    if (previousConsumedDataReceiver != null
        && !previousConsumedDataReceiver.destinationIdentifier().equals(consumedDataReceiver.destinationIdentifier())) {
      // Defensive coding. Should never happen except in case of a regression.
      throw new IllegalStateException(
          "It is not allowed to set multiple " + ConsumedDataReceiver.class.getSimpleName() + " instances for the same "
              + "topic-partition of a given consumer. Previous: " + previousConsumedDataReceiver + ", New: "
              + consumedDataReceiver);
    }
    synchronized (this) {
      notifyAll();
    }
  }

  private Rate createRate(long now) {
    Rate rate = new Rate();
    rate.init(metricConfig, now);
    return rate;
  }

  Double getMessageRate(PubSubTopicPartition topicPartition) {
    if (messageRatePerTopicPartition.containsKey(topicPartition)) {
      return messageRatePerTopicPartition.get(topicPartition).measure(metricConfig, System.currentTimeMillis());
    }
    return 0.0D;
  }

  Double getByteRate(PubSubTopicPartition topicPartition) {
    if (bytesRatePerTopicPartition.containsKey(topicPartition)) {
      return bytesRatePerTopicPartition.get(topicPartition).measure(metricConfig, System.currentTimeMillis());
    }
    return 0.0D;
  }

  Long getLastSuccessfulPollTimestamp(PubSubTopicPartition topicPartition) {
    if (lastSuccessfulPollTimestampPerTopicPartition.containsKey(topicPartition)) {
      return lastSuccessfulPollTimestampPerTopicPartition.get(topicPartition);
    }
    return DEFAULT_TOPIC_PARTITION_NO_POLL_TIMESTAMP;
  }

  void removeDataReceiver(PubSubTopicPartition topicPartition) {
    dataReceiverMap.remove(topicPartition);
  }

  /**
   * This class is used to count the number of messages and the byte size of the messages for a given store per poll.
   */
  static class StorePollCounter {
    protected int msgCount;
    protected int byteSize;

    StorePollCounter(int msgCount, int byteSize) {
      this.msgCount = msgCount;
      this.byteSize = byteSize;
    }
  }

  private Comparator<PubSubTopicPartition> getPqComparator() {
    return (o1, o2) -> {
      ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> receiver1 =
          dataReceiverMap.get(o1);
      ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> receiver2 =
          dataReceiverMap.get(o2);
      if (receiver1 == null || receiver2 == null) {
        LOGGER.error(
            "Received records from `poll` request for topic partition : {} but no data receiver found",
            receiver1 == null ? o1 : o2);
        return receiver1 == null ? -1 : 1;
      }
      return Integer.compare(receiver1.getProcessingPriority(), receiver2.getProcessingPriority());
    };
  }
}
