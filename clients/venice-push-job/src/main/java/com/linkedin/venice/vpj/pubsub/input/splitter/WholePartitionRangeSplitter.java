package com.linkedin.venice.vpj.pubsub.input.splitter;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionRangeSplit;
import com.linkedin.venice.vpj.pubsub.input.SplitRequest;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WholePartitionRangeSplitter implements PartitionRangeSplitter {
  private static final Logger LOGGER = LogManager.getLogger(WholePartitionRangeSplitter.class);

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
    LOGGER.info(
        "Created split-0 for TP-{}: count={}, start={}, end={}",
        pubSubTopicPartition,
        numberOfRecords,
        startPosition,
        endPosition);
    return Collections.singletonList(
        new PubSubPartitionRangeSplit(pubSubTopicPartition, startPosition, endPosition, numberOfRecords, 0));
  }
}
