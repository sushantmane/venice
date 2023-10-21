package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.utils.Time;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Constants used by pub-sub components.
 */
public class PubSubConstants {

  // If true, the producer will use default configuration values for optimized high throughput producing if they are not
  // explicitly set.
  public static final String PUBSUB_PRODUCER_USE_HIGH_THROUGHPUT_DEFAULTS =
      "pubsub.producer.use.high.throughput.defaults";

  public static final long PUBSUB_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS_DEFAULT_VALUE = 600;
  public static final long UNKNOWN_TOPIC_RETENTION = Long.MIN_VALUE;

  // TopicManager constants
  public static final int FAST_KAFKA_OPERATION_TIMEOUT_MS = Time.MS_PER_SECOND;
  public static final long ETERNAL_TOPIC_RETENTION_POLICY_MS = Long.MAX_VALUE;
  public static final long DEFAULT_TOPIC_RETENTION_POLICY_MS = 5 * Time.MS_PER_DAY;
  public static final long BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN = 2 * Time.MS_PER_DAY;
  public static final int DEFAULT_PUBSUB_OPERATION_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;
  public static final int MAX_TOPIC_DELETE_RETRIES = 3;
  public static final int DEFAULT_KAFKA_REPLICATION_FACTOR = 3;
  /**
   * Default setting is that no log compaction should happen for hybrid store version topics
   * if the messages are produced within 24 hours; otherwise servers could encounter MISSING
   * data DIV errors for reprocessing jobs which could potentially generate lots of
   * duplicate keys.
   */
  public static final long DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS = 24 * Time.MS_PER_HOUR;
  public static final List<Class<? extends Throwable>> CREATE_TOPIC_RETRIABLE_EXCEPTIONS =
      Collections.unmodifiableList(Arrays.asList(PubSubOpTimeoutException.class, PubSubClientRetriableException.class));
  /**
   * Default value of sleep interval for polling topic deletion status from ZK.
   */
  public static final int DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS = 2 * Time.MS_PER_SECOND;
}
