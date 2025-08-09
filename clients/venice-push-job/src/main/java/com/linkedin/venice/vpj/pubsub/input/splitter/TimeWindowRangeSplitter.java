package com.linkedin.venice.vpj.pubsub.input.splitter;

import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionRangeSplit;
import com.linkedin.venice.vpj.pubsub.input.SplitRequest;
import java.util.List;


public class TimeWindowRangeSplitter implements PartitionRangeSplitter {
  @Override
  public List<PubSubPartitionRangeSplit> split(SplitRequest splitRequest) {
    throw new UnsupportedOperationException("TimeWindowRangeSplitter is not supported");
  }
}
