package com.linkedin.venice.vpj.pubsub.input.splitter;

import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionRangeSplit;
import com.linkedin.venice.vpj.pubsub.input.SplitRequest;
import java.util.List;


public interface PartitionRangeSplitter {
  List<PubSubPartitionRangeSplit> split(SplitRequest splitRequest);
}
