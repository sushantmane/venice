package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.stats.TopicCleanupServiceStats;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The topic cleanup in Venice adopts the following strategy:
 * 1. When Controller needs to clean up topics for retired versions or uncompleted store pushes or store deletion, it only
 * truncates the topics (lower topic retention) instead of deleting them right away.
 * 2. The {@link TopicCleanupService} is working as a single process to clean up all the unused topics.
 * With this way, most of time (no leadership handover), there is only one controller talking to Kafka to delete topic, which is expected
 * from Kafka's perspective to avoid concurrent topic deletion.
 * In theory, it is still possible to have two controllers talking to Kafka to delete topic during leadership handover since
 * the previous leader controller could be still working on the topic cleaning up but the new leader controller starts
 * processing.
 *
 * If required, there might be several ways to alleviate this potential concurrent Kafka topic deletion:
 * 1. Do leader controller check every time when deleting topic;
 * 2. Register a callback to monitor leadership change;
 * 3. Use a global Zookeeper lock;
 *
 * Right now, {@link TopicCleanupService} is fully decoupled from {@link com.linkedin.venice.meta.Store} since there is
 * only one process actively running to cleanup topics and the controller running this process may not be the leader
 * controller of the cluster owning the store that the topic to be deleted belongs to.
 *
 *
 * Here is how {@link TopicCleanupService} works to clean up deprecated topics [topic with low retention policy]:
 * 1. This service is only running in leader controller of controller cluster, which means there should be only one
 * topic cleanup service running among all the Venice clusters (not strictly considering leader handover.);
 * 2. This service is running in a infinite loop, which will execute the following operations:
 *    2.1 For every round, check whether current controller is the leader controller of controller parent.
 *        If yes, continue; Otherwise, sleep for a pre-configured period and check again;
 *    2.2 Collect all the topics and categorize them based on store names;
 *    2.3 For deprecated real-time topic, will remove it right away;
 *    2.4 For deprecated version topics, will keep pre-configured minimal unused topics to avoid MM crash and remove others;
 */
public class TopicCleanupService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(TopicCleanupService.class);
  private static final Map<String, Integer> storeToCountdownForDeletion = new HashMap<>();

  private final Admin admin;
  private final Thread cleanupThread;
  protected final long sleepIntervalBetweenTopicListFetchMs;
  protected final int delayFactor;
  private final int minNumberOfUnusedKafkaTopicsToPreserve;
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final PubSubTopicRepository pubSubTopicRepository;
  private final TopicCleanupServiceStats topicCleanupServiceStats;
  private volatile boolean isRTTopicDeletionBlocked = false;
  private boolean isLeaderControllerOfControllerCluster = false;
  protected final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final ChildRegions childRegions;

  public TopicCleanupService(
      Admin admin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      PubSubTopicRepository pubSubTopicRepository,
      TopicCleanupServiceStats topicCleanupServiceStats) {
    this.admin = admin;
    this.sleepIntervalBetweenTopicListFetchMs =
        multiClusterConfigs.getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    this.delayFactor = multiClusterConfigs.getTopicCleanupDelayFactor();
    this.minNumberOfUnusedKafkaTopicsToPreserve = multiClusterConfigs.getMinNumberOfUnusedKafkaTopicsToPreserve();
    this.cleanupThread = new Thread(new TopicCleanupTask(), "TopicCleanupTask");
    this.multiClusterConfigs = multiClusterConfigs;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.topicCleanupServiceStats = topicCleanupServiceStats;
    this.childRegions = getChildRegions(
        Utils.parseCommaSeparatedStringToList(multiClusterConfigs.getCommonConfig().getChildDatacenters()));
  }

  private ChildRegions getChildRegions(List<String> childFabricList) {
    String localPubSubAddress = getTopicManager().getPubSubClusterAddress();
    ChildRegions childRegions = new ChildRegions();
    for (String regionName: childFabricList) {
      String pubSubAddress = multiClusterConfigs.getChildDataCenterKafkaUrlMap().get(regionName);
      if (localPubSubAddress.equals(pubSubAddress)) {
        childRegions.setLocalRegion(regionName, pubSubAddress);
        continue;
      }
      childRegions.addRegion(regionName, pubSubAddress);
    }
    return childRegions;
  }

  @Override
  public boolean startInner() throws Exception {
    cleanupThread.start();
    LOGGER.info("TopicCleanupService started - regions: {}", childRegions);
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    cleanupThread.interrupt();
  }

  TopicManager getTopicManager() {
    return admin.getTopicManager();
  }

  TopicManager getTopicManager(String kafkaBootstrapServerAddress) {
    return admin.getTopicManager(kafkaBootstrapServerAddress);
  }

  private class TopicCleanupTask implements Runnable {
    @Override
    public void run() {
      while (!stop.get()) {
        try {
          Thread.sleep(sleepIntervalBetweenTopicListFetchMs);
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException during sleep in TopicCleanup thread");
          break;
        }
        if (stop.get()) {
          break;
        }
        try {
          if (admin.isLeaderControllerOfControllerCluster()) {
            if (!isLeaderControllerOfControllerCluster) {
              /**
               * Sleep for some time when current controller firstly becomes leader controller of controller cluster.
               * This is trying to avoid concurrent Kafka topic deletion sent by both previous leader controller
               * (Kafka topic cleanup doesn't finish in a short period) and the new leader controller.
               */
              isLeaderControllerOfControllerCluster = true;
              LOGGER.info("Current controller becomes the leader controller of controller cluster");
              continue;
            }
            cleanupVeniceTopics();
          } else {
            isLeaderControllerOfControllerCluster = false;
          }
        } catch (Exception e) {
          LOGGER.error("Received exception when cleaning up topics", e);
        }
      }
      LOGGER.info("TopicCleanupTask stopped");
    }
  }

  Admin getAdmin() {
    return admin;
  }

  /**
   * The following will delete topics based on their priority. Real-time topics are given higher priority than version topics.
   * If version topic deletion takes more than certain time it refreshes the entire topic list and start deleting from RT topics again.
    */
  void cleanupVeniceTopics() {
    DeprecatedTopics deprecatedTopics = getTopicsToDelete();
    topicCleanupServiceStats.recordDeletableTopicsCount(deprecatedTopics.size());
    // delete RTs
    if (!isRTTopicDeletionBlocked) {
      deleteRealTimeTopics(deprecatedTopics);
    }
    // delete VTs
    for (PubSubTopic versionTopic: deprecatedTopics.getVersionTopics()) {
      deleteTopic(versionTopic);
    }
    isRTTopicDeletionBlocked = false;
  }

  void deleteTopic(PubSubTopic topic) {
    try {
      getTopicManager().ensureTopicIsDeletedAndBlockWithRetry(topic);
      topicCleanupServiceStats.recordTopicDeleted();
    } catch (Exception e) {
      LOGGER.warn("Caught exception when trying to delete topic: {} - {}", topic.getName(), e);
      topicCleanupServiceStats.recordTopicDeletionError();
      // No op, will try again in the next cleanup cycle.
    }
  }

  void deleteRealTimeTopics(DeprecatedTopics deprecatedTopics) {
    // delete RTs
    for (RealTimeTopicDeletionContext deletionContext: deprecatedTopics.getRtDeletionContexts().values()) {
      if (deletionContext.isOkToDelete()) {
        deleteTopic(deletionContext.getRealTimeTopic());
        continue;
      }

      LOGGER.info(
          "Topic deletion for topic: {} is delayed due version topics found in datacenter: {}",
          deletionContext.getRealTimeTopic().getName(),
          deletionContext.getPerFabricVersionTopicCount());
    }
  }

  private DeprecatedTopics getTopicsToDelete() {
    Map<PubSubTopic, Long> allLocalTopicRetentions = getTopicManager().getAllTopicRetentions();
    DeprecatedTopics deprecatedTopics = getDeprecatedTopics(allLocalTopicRetentions);
    if (admin.isParent()) {
      return deprecatedTopics;
    }

    // Fill in version topic count for stores associated with real-time topics marked for deletion.
    Set<String> rtAssociatedStores =
        deprecatedTopics.getRealTimeTopics().stream().map(PubSubTopic::getStoreName).collect(Collectors.toSet());
    Set<PubSubTopic> allLocalTopics = allLocalTopicRetentions.keySet();
    Map<String, Integer> storeToVersionTopicCount = getPerStoreVersionTopicCount(allLocalTopics, rtAssociatedStores);
    fillInVtCountsForDeletableRts(childRegions.getLocalRegionName(), deprecatedTopics, storeToVersionTopicCount);
    isRTTopicDeletionBlocked = false;
    childRegions.getRemoteChildRegionToPubSubAddress().entrySet().parallelStream().forEach(entry -> {
      try {
        Set<PubSubTopic> allRemoteTopics = getTopicManager(entry.getValue()).listTopics();
        Map<String, Integer> perFabricVersionTopicCount =
            getPerStoreVersionTopicCount(allRemoteTopics, rtAssociatedStores);
        fillInVtCountsForDeletableRts(entry.getKey(), deprecatedTopics, perFabricVersionTopicCount);
      } catch (Exception e) {
        LOGGER.error("Failed to fetch topics from remote fabric: {}", entry.getKey(), e);
        isRTTopicDeletionBlocked = true;
      }
    });

    return deprecatedTopics;
  }

  private DeprecatedTopics getDeprecatedTopics(Map<PubSubTopic, Long> topicRetentions) {
    Map<String, Map<PubSubTopic, Long>> storeTopicRetentionMap = buildStoreTopicRetentionMap(topicRetentions);
    DeprecatedTopics deprecatedTopics = new DeprecatedTopics();
    for (Map.Entry<String, Map<PubSubTopic, Long>> entry: storeTopicRetentionMap.entrySet()) {
      String storeName = entry.getKey();
      Map<PubSubTopic, Long> retentions = entry.getValue();
      int preserveCount = minNumberOfUnusedKafkaTopicsToPreserve;
      PubSubTopic rt = pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(storeName));
      if (retentions.containsKey(rt) && admin.isTopicTruncatedBasedOnRetention(retentions.get(rt))) {
        deprecatedTopics.addRt(rt);
        preserveCount = 0;
      }
      deprecatedTopics.addVersionTopics(extractVersionTopicsToCleanup(admin, retentions, preserveCount, delayFactor));
    }
    return deprecatedTopics;
  }

  void fillInVtCountsForDeletableRts(
      String region,
      DeprecatedTopics deprecatedTopics,
      Map<String, Integer> storeToVtCount) {
    Set<PubSubTopic> realTimeTopics = deprecatedTopics.getRealTimeTopics();
    for (PubSubTopic rt: realTimeTopics) {
      RealTimeTopicDeletionContext context = deprecatedTopics.getRtDeletionContexts().get(rt);
      if (context == null) {
        LOGGER.error("Real-time topic {} is missing from the context map", rt.getName());
        continue;
      }
      if (storeToVtCount.containsKey(rt.getStoreName())) {
        context.addPerFabricVersionTopicCount(region, storeToVtCount.get(rt.getStoreName()));
      }
    }
  }

  private static Map<String, Integer> getPerStoreVersionTopicCount(
      Set<PubSubTopic> topics,
      Set<String> filterStoreNames) {
    Map<String, Integer> storeToVersionTopicCount = new HashMap<>(filterStoreNames.size());
    for (PubSubTopic topic: topics) {
      if (filterStoreNames.contains(topic.getStoreName()) && topic.isVersionTopic()) {
        storeToVersionTopicCount.merge(topic.getStoreName(), 1, Integer::sum);
      }
    }
    return storeToVersionTopicCount;
  }

  /**
   * Builds a map that connects each store name with its corresponding pub-sub topics
   * and their configured retention times.
   *
   * @param topicRetentions a map containing pub-sub topics and their respective retention times
   * @return a map associating store names with their pub-sub topics and retention times
   */
  public static Map<String, Map<PubSubTopic, Long>> buildStoreTopicRetentionMap(
      Map<PubSubTopic, Long> topicRetentions) {
    Map<String, Map<PubSubTopic, Long>> storeTopicRetentionMap = new HashMap<>();

    for (Map.Entry<PubSubTopic, Long> entry: topicRetentions.entrySet()) {
      PubSubTopic topic = entry.getKey();
      long retention = entry.getValue();
      String storeName = topic.getStoreName();

      // Skip topics not belonging to a store
      if (storeName == null || storeName.isEmpty()) {
        continue;
      }

      storeTopicRetentionMap.computeIfAbsent(storeName, k -> new HashMap<>()).put(topic, retention);
    }
    return storeTopicRetentionMap;
  }

  /**
   * Filter Venice version topics so that the returned topics satisfying the following conditions:
   * <ol>
   *  <li> topic is truncated based on retention time. </li>
   *  <li> topic version is in the deletion range. </li>
   *  <li> current controller is the parent controller or Helix resource for this topic is already removed in child controller. </li>
   *  <li> topic is a real time topic;
   *     <p>
   *       or topic is a version topic and passes delay countdown condition. </li>
   * </ol>
   * @return a list that contains topics satisfying all the above conditions.
   */
  public static List<PubSubTopic> extractVersionTopicsToCleanup(
      Admin admin,
      Map<PubSubTopic, Long> topicRetentions,
      int minNumberOfUnusedKafkaTopicsToPreserve,
      int delayFactor) {
    if (topicRetentions.isEmpty()) {
      return Collections.emptyList();
    }
    Set<PubSubTopic> veniceTopics = topicRetentions.keySet();
    Optional<Integer> optionalMaxVersion = veniceTopics.stream()
        .filter(PubSubTopic::isVersionTopic)
        .map(vt -> Version.parseVersionFromKafkaTopicName(vt.getName()))
        .max(Integer::compare);

    if (!optionalMaxVersion.isPresent()) {
      return Collections.emptyList();
    }

    int maxVersion = optionalMaxVersion.get();

    String storeName = veniceTopics.iterator().next().getStoreName();
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    boolean isStoreZkShared = systemStoreType != null && systemStoreType.isStoreZkShared();
    boolean isStoreDeleted = !isStoreZkShared && !admin.getStoreConfigRepo().getStoreConfig(storeName).isPresent();
    // Do not preserve any VT for deleted user stores or zk shared system stores.
    // TODO revisit the behavior if we'd like to support rollback for zk shared system stores.
    final long maxVersionNumberToDelete =
        isStoreDeleted || isStoreZkShared ? maxVersion : maxVersion - minNumberOfUnusedKafkaTopicsToPreserve;

    return veniceTopics.stream()
        .filter(PubSubTopic::isVersionTopic)
        /** Consider only truncated topics */
        .filter(t -> admin.isTopicTruncatedBasedOnRetention(topicRetentions.get(t)))
        /** Always preserve the last {@link #minNumberOfUnusedKafkaTopicsToPreserve} topics, whether they are healthy or not */
        .filter(t -> Version.parseVersionFromKafkaTopicName(t.getName()) <= maxVersionNumberToDelete)
        /**
         * Filter out resources, which haven't been fully removed in child fabrics yet. This is only performed in the
         * child fabric because parent fabric don't have storage node helix resources.
         *
         * The reason to filter out still-alive resource is to avoid triggering the non-existing topic issue
         * of Kafka consumer happening in Storage Node.
         */
        .filter(t -> admin.isParent() || !admin.isResourceStillAlive(t.getName()))
        .filter(t -> {
          if (Version.isRealTimeTopic(t.getName())) {
            return true;
          }
          // delay VT topic deletion as there could be a race condition where the resource is already deleted by venice
          // but kafka still holding on to the deleted topic message in producer buffer which might cause infinite hang
          // in kafka.
          int remainingFactor =
              storeToCountdownForDeletion.merge(t.getName(), delayFactor, (oldVal, givenVal) -> oldVal - 1);
          if (remainingFactor > 0) {
            return false;
          }
          storeToCountdownForDeletion.remove(t);
          return true;
        })
        .collect(Collectors.toList());
  }

  static class DeprecatedTopics {
    private final Set<PubSubTopic> versionTopics = ConcurrentHashMap.newKeySet();
    private final Map<PubSubTopic, RealTimeTopicDeletionContext> realTimeTopics = new ConcurrentHashMap<>(3);

    void addRt(PubSubTopic topic) {
      realTimeTopics.put(topic, new RealTimeTopicDeletionContext(topic));
    }

    void addVersionTopics(List<PubSubTopic> topics) {
      versionTopics.addAll(topics);
    }

    Set<PubSubTopic> getRealTimeTopics() {
      return realTimeTopics.keySet();
    }

    Map<PubSubTopic, RealTimeTopicDeletionContext> getRtDeletionContexts() {
      return realTimeTopics;
    }

    Set<PubSubTopic> getVersionTopics() {
      return versionTopics;
    }

    int size() {
      return realTimeTopics.size() + versionTopics.size();
    }
  }

  static class RealTimeTopicDeletionContext {
    private PubSubTopic realTimeTopic;
    private final Map<String, Integer> perFabricVersionTopicCount;

    RealTimeTopicDeletionContext(PubSubTopic realTimeTopic) {
      if (!realTimeTopic.isRealTime()) {
        throw new IllegalArgumentException("RealTimeTopicDeletionContext can only be created for real-time topics");
      }
      this.realTimeTopic = realTimeTopic;
      this.perFabricVersionTopicCount = new ConcurrentHashMap<>(3);
    }

    Map<String, Integer> getPerFabricVersionTopicCount() {
      return perFabricVersionTopicCount;
    }

    void addPerFabricVersionTopicCount(String region, int count) {
      perFabricVersionTopicCount.put(region, count);
    }

    PubSubTopic getRealTimeTopic() {
      return realTimeTopic;
    }

    boolean isOkToDelete() {
      // If all version topics are deleted in all child fabrics, then it is safe to delete the real-time topic.
      return perFabricVersionTopicCount.values().stream().allMatch(count -> count == 0);
    }
  }

  static class ChildRegions {
    private final Map<String, String> remoteChildRegionToPubSubAddress = new HashMap<>();
    private String localRegionName;
    private String localPubSubAddress;

    public void addRegion(String region, String pubSubAddress) {
      remoteChildRegionToPubSubAddress.put(region, pubSubAddress);
    }

    public void setLocalRegion(String localRegionName, String pubSubAddress) {
      this.localRegionName = Objects.requireNonNull(localRegionName, "Local region name cannot be null");
      this.localPubSubAddress = Objects.requireNonNull(pubSubAddress, "Local pubsub address cannot be null");
    }

    public Map<String, String> getRemoteChildRegionToPubSubAddress() {
      return remoteChildRegionToPubSubAddress;
    }

    public String getLocalRegionName() {
      return localRegionName;
    }

    @Override
    public String toString() {
      return "ChildRegions{" + "localRegionName='" + localRegionName + '\'' + ", remoteRegions="
          + remoteChildRegionToPubSubAddress.keySet() + '}';
    }
  }
}
