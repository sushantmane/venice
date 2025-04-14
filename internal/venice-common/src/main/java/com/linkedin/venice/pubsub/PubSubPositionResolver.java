package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class to resolve {@link PubSubPosition} instances from raw position bytes,
 * using the appropriate {@link TopicManager} based on either the broker address or
 * a PubSub cluster ID.
 */
public class PubSubPositionResolver {
  private static final Logger LOGGER = LogManager.getLogger(PubSubPositionResolver.class);

  /**
   * Mapping from position type ID to their corresponding position resolver (not currently used).
   */
  private final Map<Integer, PubSubPositionResolver> positionResolversByType = new VeniceConcurrentHashMap<>(4);

  /**
   * Repository that provides access to {@link TopicManager} instances by broker address.
   */
  private final TopicManagerRepository topicManagerRepository;

  /**
   * Mapping from PubSub cluster ID to its corresponding broker address.
   */
  private final Int2ObjectMap<String> clusterIdToBrokerAddressMap;

  /**
   * Constructs a {@link PubSubPositionResolver} using the given context.
   *
   * @param context the resolver context containing dependencies like the topic manager repository and cluster mapping
   */
  public PubSubPositionResolver(PubSubPositionResolverContext context) {
    this.topicManagerRepository = context.getTopicManagerRepository();
    this.clusterIdToBrokerAddressMap = context.getPubSubClusterIdToUrlMap();
  }

  /**
   * Resolves a {@link PubSubPosition} from the given broker address, topic partition, and position bytes.
   *
   * @param brokerAddress   the address of the PubSub broker
   * @param topicPartition  the topic and partition to resolve the position for
   * @param positionBytes   the serialized position bytes
   * @return the resolved {@link PubSubPosition}
   */
  public PubSubPosition resolvePositionUsingBrokerAddress(
      String brokerAddress,
      PubSubTopicPartition topicPartition,
      byte[] positionBytes) {
    TopicManager topicManager = topicManagerRepository.getTopicManager(brokerAddress);
    return topicManager.resolvePosition(topicPartition, positionBytes);
  }

  /**
   * Resolves a {@link PubSubPosition} using the PubSub cluster ID, topic partition, and position bytes.
   * Throws an exception if the cluster ID is not recognized.
   *
   * @param clusterId       the PubSub cluster ID
   * @param topicPartition  the topic and partition to resolve the position for
   * @param positionBytes   the serialized position bytes
   * @return the resolved {@link PubSubPosition}
   * @throws IllegalArgumentException if the cluster ID is not found
   */
  public PubSubPosition resolvePositionUsingClusterId(
      int clusterId,
      PubSubTopicPartition topicPartition,
      byte[] positionBytes) {

    String brokerAddress = clusterIdToBrokerAddressMap.get(clusterId);
    if (brokerAddress == null) {
      LOGGER.error(
          "Unable to resolve PubSubPosition for topic partition {} with cluster ID {}: Cluster ID not found in mapping: {}",
          topicPartition,
          clusterId,
          clusterIdToBrokerAddressMap);
      throw new IllegalArgumentException("PubSub cluster ID " + clusterId + " not found in mapping");
    }

    TopicManager topicManager = topicManagerRepository.getTopicManager(brokerAddress);
    return topicManager.resolvePosition(topicPartition, positionBytes);
  }
}
