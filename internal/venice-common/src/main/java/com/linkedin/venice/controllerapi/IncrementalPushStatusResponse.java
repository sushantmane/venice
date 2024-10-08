package com.linkedin.venice.controllerapi;

import java.util.Map;


/**
 * A controller response for incremental push status requests.
 */
public class IncrementalPushStatusResponse extends ControllerResponse {
  private Map<Integer, Map<String, String>> partitionStatuses;

  public Map<Integer, Map<String, String>> getPartitionStatuses() {
    return partitionStatuses;
  }

  public void setPartitionStatuses(Map<Integer, Map<String, String>> partitionStatuses) {
    this.partitionStatuses = partitionStatuses;
  }
}
