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


public class CappedSplitCountRangeSplitter implements PartitionRangeSplitter {
  private static final Logger LOGGER = LogManager.getLogger(CappedSplitCountRangeSplitter.class);

  @Override
  public List<PubSubPartitionRangeSplit> split(SplitRequest splitRequest) {
    PubSubTopicPartition pubSubTopicPartition = splitRequest.getPubSubTopicPartition();
    TopicManager topicManager = splitRequest.getTopicManager();
    int maxSplits = Math.max(1, splitRequest.getMaxSplits()); // ensure >= 1

    PubSubPosition startPosition = topicManager.getStartPositionsForPartition(pubSubTopicPartition);
    PubSubPosition endPosition = topicManager.getEndPositionsForPartition(pubSubTopicPartition);

    long numberOfRecords = topicManager.diffPosition(pubSubTopicPartition, endPosition, startPosition);
    if (numberOfRecords <= 0) {
      return Collections.emptyList();
    }

    // You cannot have more splits than records.
    int splits = (int) Math.min(numberOfRecords, maxSplits);

    long base = numberOfRecords / splits; // >= 1
    long remainder = numberOfRecords % splits; // first 'remainder' splits get +1

    List<PubSubPartitionRangeSplit> out = new ArrayList<>(splits);
    PubSubPosition curStart = startPosition;

    for (int i = 0; i < splits; i++) {
      long thisSplitCount = base + (i < remainder ? 1 : 0);

      // Advance from curStart by thisSplitCount to get the exclusive end.
      PubSubPosition curEnd = topicManager.advancePosition(pubSubTopicPartition, curStart, thisSplitCount);

      out.add(
          new PubSubPartitionRangeSplit(
              pubSubTopicPartition,
              curStart, // inclusive
              curEnd, // exclusive
              thisSplitCount,
              i));
      LOGGER.info(
          "Created split-{} for TP-{}: count={}, start={}, end={}",
          i,
          pubSubTopicPartition,
          thisSplitCount,
          curStart,
          curEnd);
      // Next segment starts where this one ended.
      curStart = curEnd;
    }

    return out;
  }
}
