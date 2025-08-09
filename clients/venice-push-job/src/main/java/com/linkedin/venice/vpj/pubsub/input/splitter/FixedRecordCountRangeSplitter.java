package com.linkedin.venice.vpj.pubsub.input.splitter;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionRangeSplit;
import com.linkedin.venice.vpj.pubsub.input.SplitRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class FixedRecordCountRangeSplitter implements PartitionRangeSplitter {
  private static final Logger LOGGER = LogManager.getLogger(FixedRecordCountRangeSplitter.class);

  @Override
  public List<PubSubPartitionRangeSplit> split(SplitRequest splitRequest) {
    PubSubTopicPartition pubSubTopicPartition = splitRequest.getPubSubTopicPartition();
    TopicManager topicManager = splitRequest.getTopicManager();

    PubSubPosition startPosition = topicManager.getStartPositionsForPartition(pubSubTopicPartition);
    PubSubPosition endPosition = topicManager.getEndPositionsForPartition(pubSubTopicPartition);

    long numberOfRecords = topicManager.diffPosition(pubSubTopicPartition, endPosition, startPosition);
    if (numberOfRecords <= 0) {
      return Collections.emptyList();
    }

    long recordsPerSplit = splitRequest.getRecordsPerSplit();
    if (recordsPerSplit <= 0) {
      throw new IllegalArgumentException("recordsPerSplit must be > 0, got " + recordsPerSplit);
    }

    // Compute number of splits (ceil division).
    long splitsLong = (numberOfRecords + recordsPerSplit - 1) / recordsPerSplit;
    int splits = (splitsLong > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) splitsLong;

    List<PubSubPartitionRangeSplit> out = new ArrayList<>(splits);

    PubSubPosition curStart = startPosition;
    long remaining = numberOfRecords;
    int rangeIndex = 0;

    while (remaining > 0 && rangeIndex < splits) {
      long thisSplitCount = Math.min(recordsPerSplit, remaining);

      // Exclusive end = advance from curStart by thisSplitCount
      PubSubPosition curEnd = topicManager.advancePosition(pubSubTopicPartition, curStart, thisSplitCount);

      out.add(
          new PubSubPartitionRangeSplit(
              pubSubTopicPartition,
              curStart, // inclusive
              curEnd, // exclusive
              thisSplitCount, // numberOfRecords
              rangeIndex)); // rangeIndex
      LOGGER.info(
          "Created split-{} for TP-{}: count={}, start={}, end={}",
          rangeIndex,
          pubSubTopicPartition,
          thisSplitCount,
          curStart,
          curEnd);

      // Next iteration
      curStart = curEnd;
      remaining -= thisSplitCount;
      rangeIndex++;
    }

    return out;
  }
}
