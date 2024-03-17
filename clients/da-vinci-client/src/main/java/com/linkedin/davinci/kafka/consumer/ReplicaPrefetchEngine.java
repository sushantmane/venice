package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.message.KafkaKey;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ReplicaPrefetchEngine {
  private static final Logger LOGGER = LogManager.getLogger(ReplicaPrefetchEngine.class);
  static final RecordPrefetchContext EMPTY_RPC = new RecordPrefetchContext("PLACE-HOLDER", -1, Collections.emptyList());

  static class RecordPrefetchContext {
    private final String versionTopic;
    private final int partition;
    private final List<KafkaKey> keysToFetch;

    RecordPrefetchContext(String versionTopic, int partition, List<KafkaKey> keysToFetch) {
      this.versionTopic = versionTopic;
      this.partition = partition;
      this.keysToFetch = keysToFetch;
    }

    void addKey(KafkaKey key) {
      keysToFetch.add(key);
    }

    Iterator<KafkaKey> getKeysToFetch() {
      return keysToFetch.iterator();
    }

    int size() {
      return keysToFetch.size();
    }

    @Override
    public String toString() {
      return "PrefetchContext{vtp: " + versionTopic + "-" + partition + ", nKeys: " + keysToFetch.size() + "}";
    }
  }
}
