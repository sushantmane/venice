package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.stats.DisabledPartitionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.testng.annotations.Test;


public class TestVeniceHelixAdmin {
  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  @Test
  public void testDropResources() {
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    List<String> nodes = new ArrayList<>();
    String storeName = "abc";
    String instance = "node_1";
    String kafkaTopic = Version.composeKafkaTopic(storeName, 1);
    String clusterName = "venice-cluster";
    nodes.add(instance);
    Map<String, List<String>> listMap = new HashMap<>();
    List<String> partitions = new ArrayList<>(3);
    for (int partitionId = 0; partitionId < 3; partitionId++) {
      partitions.add(HelixUtils.getPartitionName(kafkaTopic, partitionId));
    }
    listMap.put(kafkaTopic, partitions);
    HelixAdminClient adminClient = mock(HelixAdminClient.class);
    HelixVeniceClusterResources veniceClusterResources = mock(HelixVeniceClusterResources.class);
    HelixExternalViewRepository repository = mock(HelixExternalViewRepository.class);
    PartitionAssignment partitionAssignment = mock(PartitionAssignment.class);
    doReturn(adminClient).when(veniceHelixAdmin).getHelixAdminClient();
    doReturn(listMap).when(adminClient).getDisabledPartitionsMap(clusterName, instance);
    doReturn(3).when(partitionAssignment).getExpectedNumberOfPartitions();
    doReturn(veniceClusterResources).when(veniceHelixAdmin).getHelixVeniceClusterResources(anyString());
    doReturn(repository).when(veniceClusterResources).getRoutingDataRepository();
    doReturn(nodes).when(veniceHelixAdmin).getStorageNodes(anyString());
    doReturn(partitionAssignment).when(repository).getPartitionAssignments(anyString());
    doReturn(mock(DisabledPartitionStats.class)).when(veniceHelixAdmin).getDisabledPartitionStats(anyString());
    doCallRealMethod().when(veniceHelixAdmin).deleteHelixResource(anyString(), anyString());

    veniceHelixAdmin.deleteHelixResource(clusterName, kafkaTopic);
    verify(veniceHelixAdmin, times(1)).enableDisabledPartition(clusterName, kafkaTopic, false);
  }

  /**
   * This test verify that in function {@link VeniceHelixAdmin#setUpMetaStoreAndMayProduceSnapshot},
   * meta store RT topic creation has to happen before any writings to meta store's rt topic.
   * As of today, topic creation and checks to make sure that RT exists are handled in function
   * {@link VeniceHelixAdmin#ensureRealTimeTopicExistsForUserSystemStores}. On the other hand, as {@link VeniceHelixAdmin#storeMetadataUpdate}
   * writes to the same RT topic, it should happen after the above function. The following test enforces
   * such order at the statement level.
   *
   * Notice that if function semantics change over time, as long as the above invariant can be obtained,
   * it is okay to relax on the ordering enforcement or delete the unit test if necessary.
   */
  @Test
  public void enforceRealTimeTopicCreationBeforeWritingToMetaSystemStore() {
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    doNothing().when(veniceHelixAdmin).ensureRealTimeTopicExistsForUserSystemStores(anyString(), anyString());
    doCallRealMethod().when(veniceHelixAdmin).setUpMetaStoreAndMayProduceSnapshot(anyString(), anyString());

    InOrder inorder = inOrder(veniceHelixAdmin);

    HelixVeniceClusterResources veniceClusterResources = mock(HelixVeniceClusterResources.class);
    ReadWriteStoreRepository repo = mock(ReadWriteStoreRepository.class);
    Store store = mock(Store.class);

    doReturn(veniceClusterResources).when(veniceHelixAdmin).getHelixVeniceClusterResources(anyString());
    doReturn(repo).when(veniceClusterResources).getStoreMetadataRepository();
    doReturn(store).when(repo).getStore(anyString());
    doReturn(Boolean.FALSE).when(store).isDaVinciPushStatusStoreEnabled();

    veniceHelixAdmin.setUpMetaStoreAndMayProduceSnapshot(anyString(), anyString());

    // Enforce that ensureRealTimeTopicExistsForUserSystemStores happens before storeMetadataUpdate. See the above
    // comments for the reasons.
    inorder.verify(veniceHelixAdmin).ensureRealTimeTopicExistsForUserSystemStores(anyString(), anyString());
    inorder.verify(veniceHelixAdmin).storeMetadataUpdate(anyString(), anyString(), any());
  }

  @Test
  public void testGetOverallPushStatus() {
    ExecutionStatus veniceStatus = ExecutionStatus.COMPLETED;
    ExecutionStatus daVinciStatus = ExecutionStatus.COMPLETED;
    ExecutionStatus overallStatus = VeniceHelixAdmin.getOverallPushStatus(veniceStatus, daVinciStatus);

    assertEquals(overallStatus, ExecutionStatus.COMPLETED);

    veniceStatus = ExecutionStatus.ERROR;
    daVinciStatus = ExecutionStatus.COMPLETED;
    overallStatus = VeniceHelixAdmin.getOverallPushStatus(veniceStatus, daVinciStatus);
    assertEquals(overallStatus, ExecutionStatus.ERROR);

    veniceStatus = ExecutionStatus.ERROR;
    daVinciStatus = ExecutionStatus.ERROR;
    overallStatus = VeniceHelixAdmin.getOverallPushStatus(veniceStatus, daVinciStatus);
    assertEquals(overallStatus, ExecutionStatus.ERROR);

    veniceStatus = ExecutionStatus.COMPLETED;
    daVinciStatus = ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
    overallStatus = VeniceHelixAdmin.getOverallPushStatus(veniceStatus, daVinciStatus);
    assertEquals(overallStatus, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);

    veniceStatus = ExecutionStatus.ERROR;
    daVinciStatus = ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
    overallStatus = VeniceHelixAdmin.getOverallPushStatus(veniceStatus, daVinciStatus);
    assertEquals(overallStatus, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
  }

  @Test
  public void testIsRealTimeTopicRequired() {
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    Store store = mock(Store.class, RETURNS_DEEP_STUBS);
    Version version = mock(Version.class);
    doCallRealMethod().when(veniceHelixAdmin).isRealTimeTopicRequired(store, version);

    // Case 1: Store is not hybrid
    doReturn(false).when(store).isHybrid();
    assertFalse(veniceHelixAdmin.isRealTimeTopicRequired(store, version));

    // Case 2: Store is hybrid and version is not hybrid
    doReturn(true).when(store).isHybrid();
    doReturn(false).when(version).isHybrid();

    // Case 3: Both store and version are hybrid && controller is child
    doReturn(true).when(store).isHybrid();
    doReturn(true).when(version).isHybrid();
    assertTrue(veniceHelixAdmin.isRealTimeTopicRequired(store, version));
    doReturn(false).when(veniceHelixAdmin).isParent();
    assertTrue(veniceHelixAdmin.isRealTimeTopicRequired(store, version));

    // Case 4: Both store and version are hybrid && controller is parent && AA is enabled
    doReturn(true).when(veniceHelixAdmin).isParent();
    doReturn(true).when(store).isActiveActiveReplicationEnabled();
    assertFalse(veniceHelixAdmin.isRealTimeTopicRequired(store, version));

    // Case 5: Both store and version are hybrid && controller is parent && AA is disabled and IncPush is enabled
    doReturn(false).when(store).isActiveActiveReplicationEnabled();
    doReturn(true).when(store).isIncrementalPushEnabled();
    when(store.getHybridStoreConfig().getDataReplicationPolicy()).thenReturn(DataReplicationPolicy.NON_AGGREGATE);
    assertTrue(veniceHelixAdmin.isRealTimeTopicRequired(store, version));

    // Case 6: Both store and version are hybrid && controller is parent && AA is disabled and IncPush is disabled but
    // DRP is AGGREGATE
    doReturn(false).when(store).isIncrementalPushEnabled();
    when(store.getHybridStoreConfig().getDataReplicationPolicy()).thenReturn(DataReplicationPolicy.AGGREGATE);
    assertTrue(veniceHelixAdmin.isRealTimeTopicRequired(store, version));
  }

  @Test
  public void testCreateOrUpdateRealTimeTopics() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    Store store = mock(Store.class, RETURNS_DEEP_STUBS);
    when(store.getName()).thenReturn(storeName);
    Version version = mock(Version.class);
    when(version.getStoreName()).thenReturn(storeName);

    // Case 1: Only one real-time topic is required
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    doCallRealMethod().when(veniceHelixAdmin).createOrUpdateRealTimeTopics(eq(clusterName), eq(store), eq(version));
    when(veniceHelixAdmin.getPubSubTopicRepository()).thenReturn(PUB_SUB_TOPIC_REPOSITORY);
    doNothing().when(veniceHelixAdmin)
        .createOrUpdateRealTimeTopic(eq(clusterName), eq(store), eq(version), any(PubSubTopic.class));
    veniceHelixAdmin.createOrUpdateRealTimeTopics(clusterName, store, version);
    // verify and capture the arguments passed to createOrUpdateRealTimeTopic
    ArgumentCaptor<PubSubTopic> pubSubTopicArgumentCaptor = ArgumentCaptor.forClass(PubSubTopic.class);
    verify(veniceHelixAdmin, times(1))
        .createOrUpdateRealTimeTopic(eq(clusterName), eq(store), eq(version), pubSubTopicArgumentCaptor.capture());
    assertEquals(pubSubTopicArgumentCaptor.getValue().getName(), "testStore_rt");

    // Case 2: Both regular and separate real-time topics are required
    when(version.isSeparateRealTimeTopicEnabled()).thenReturn(true);
    veniceHelixAdmin.createOrUpdateRealTimeTopics(clusterName, store, version);
    pubSubTopicArgumentCaptor = ArgumentCaptor.forClass(PubSubTopic.class);
    // verify and capture the arguments passed to createOrUpdateRealTimeTopic
    verify(veniceHelixAdmin, times(3))
        .createOrUpdateRealTimeTopic(eq(clusterName), eq(store), eq(version), pubSubTopicArgumentCaptor.capture());
    Set<PubSubTopic> pubSubTopics = new HashSet<>(pubSubTopicArgumentCaptor.getAllValues());
    PubSubTopic separateRealTimeTopic = PUB_SUB_TOPIC_REPOSITORY.getTopic(storeName + "_rt_sep");
    assertTrue(pubSubTopics.contains(separateRealTimeTopic));
  }

  @Test
  public void testCreateOrUpdateRealTimeTopic() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    int partitionCount = 10;
    Store store = mock(Store.class, RETURNS_DEEP_STUBS);
    when(store.getName()).thenReturn(storeName);
    Version version = mock(Version.class);
    when(version.getStoreName()).thenReturn(storeName);
    when(version.getPartitionCount()).thenReturn(partitionCount);
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(storeName + "_rt");
    TopicManager topicManager = mock(TopicManager.class);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    when(veniceHelixAdmin.getTopicManager()).thenReturn(topicManager);

    // Case 1: Real-time topic already exists
    doCallRealMethod().when(veniceHelixAdmin)
        .createOrUpdateRealTimeTopic(eq(clusterName), eq(store), eq(version), any(PubSubTopic.class));
    when(veniceHelixAdmin.getPubSubTopicRepository()).thenReturn(pubSubTopicRepository);
    when(topicManager.containsTopic(pubSubTopic)).thenReturn(true);
    doNothing().when(veniceHelixAdmin)
        .validateAndUpdateTopic(eq(pubSubTopic), eq(store), eq(version), eq(partitionCount), eq(topicManager));
    veniceHelixAdmin.createOrUpdateRealTimeTopic(clusterName, store, version, pubSubTopic);
    verify(veniceHelixAdmin, times(1))
        .validateAndUpdateTopic(eq(pubSubTopic), eq(store), eq(version), eq(partitionCount), eq(topicManager));
    verify(topicManager, never()).createTopic(
        any(PubSubTopic.class),
        anyInt(),
        anyInt(),
        anyLong(),
        anyBoolean(),
        any(Optional.class),
        anyBoolean());

    // Case 2: Real-time topic does not exist
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(veniceHelixAdmin.getControllerConfig(clusterName)).thenReturn(clusterConfig);
    when(topicManager.containsTopic(pubSubTopic)).thenReturn(false);
    veniceHelixAdmin.createOrUpdateRealTimeTopic(clusterName, store, version, pubSubTopic);
    verify(topicManager, times(1)).createTopic(
        eq(pubSubTopic),
        eq(partitionCount),
        anyInt(),
        anyLong(),
        anyBoolean(),
        any(Optional.class),
        anyBoolean());
  }

  @Test
  public void testValidateAndUpdateTopic() {
    PubSubTopic realTimeTopic = PUB_SUB_TOPIC_REPOSITORY.getTopic("testStore_rt");
    Store store = mock(Store.class, RETURNS_DEEP_STUBS);
    when(store.getName()).thenReturn("testStore");
    Version version = mock(Version.class);
    int expectedNumOfPartitions = 10;
    TopicManager topicManager = mock(TopicManager.class);

    // Case 1: Actual partition count is not equal to expected partition count
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    doCallRealMethod().when(veniceHelixAdmin)
        .validateAndUpdateTopic(
            any(PubSubTopic.class),
            any(Store.class),
            any(Version.class),
            anyInt(),
            any(TopicManager.class));
    when(version.getPartitionCount()).thenReturn(expectedNumOfPartitions);
    when(topicManager.getPartitionCount(realTimeTopic)).thenReturn(expectedNumOfPartitions - 1);
    Exception exception = expectThrows(
        VeniceException.class,
        () -> veniceHelixAdmin
            .validateAndUpdateTopic(realTimeTopic, store, version, expectedNumOfPartitions, topicManager));
    assertTrue(exception.getMessage().contains("has different partition count"));

    // Case 2: Actual partition count is equal to expected partition count
    when(topicManager.getPartitionCount(realTimeTopic)).thenReturn(expectedNumOfPartitions);
    when(topicManager.updateTopicRetentionWithRetries(eq(realTimeTopic), anyLong())).thenReturn(true);
    veniceHelixAdmin.validateAndUpdateTopic(realTimeTopic, store, version, expectedNumOfPartitions, topicManager);
    verify(topicManager, times(1)).updateTopicRetentionWithRetries(eq(realTimeTopic), anyLong());
  }

  @Test
  public void testEnsureRealTimeTopicExistsForUserSystemStores() {
    String clusterName = "testCluster";
    String storeName = "testStore";
    String systemStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
    int partitionCount = 10;
    Store userStore = mock(Store.class, RETURNS_DEEP_STUBS);
    when(userStore.getName()).thenReturn(storeName);
    Version version = mock(Version.class);
    when(version.getStoreName()).thenReturn(storeName);
    when(version.getPartitionCount()).thenReturn(partitionCount);
    when(userStore.getPartitionCount()).thenReturn(partitionCount);
    TopicManager topicManager = mock(TopicManager.class);
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    doReturn(topicManager).when(veniceHelixAdmin).getTopicManager();
    doReturn(pubSubTopicRepository).when(veniceHelixAdmin).getPubSubTopicRepository();

    // Case 1: Store does not exist
    doReturn(null).when(veniceHelixAdmin).getStore(clusterName, storeName);
    doNothing().when(veniceHelixAdmin).checkControllerLeadershipFor(clusterName);
    doCallRealMethod().when(veniceHelixAdmin).ensureRealTimeTopicExistsForUserSystemStores(anyString(), anyString());
    Exception notFoundException = expectThrows(
        VeniceException.class,
        () -> veniceHelixAdmin.ensureRealTimeTopicExistsForUserSystemStores(clusterName, storeName));
    assertTrue(
        notFoundException.getMessage().contains("does not exist in"),
        "Actual message: " + notFoundException.getMessage());

    // Case 2: Store exists, but it's not user system store
    doReturn(userStore).when(veniceHelixAdmin).getStore(clusterName, storeName);
    Exception notUserSystemStoreException = expectThrows(
        VeniceException.class,
        () -> veniceHelixAdmin.ensureRealTimeTopicExistsForUserSystemStores(clusterName, storeName));
    assertTrue(
        notUserSystemStoreException.getMessage().contains("is not a user system store"),
        "Actual message: " + notUserSystemStoreException.getMessage());

    // Case 3: Store exists, it's a user system store, but real-time topic already exists
    Store systemStore = mock(Store.class, RETURNS_DEEP_STUBS);
    doReturn(systemStoreName).when(systemStore).getName();
    doReturn(Collections.emptyList()).when(systemStore).getVersions();
    doReturn(systemStore).when(veniceHelixAdmin).getStore(clusterName, systemStoreName);
    doReturn(true).when(topicManager).containsTopic(any(PubSubTopic.class));
    veniceHelixAdmin.ensureRealTimeTopicExistsForUserSystemStores(clusterName, systemStoreName);
    verify(topicManager, times(1)).containsTopic(any(PubSubTopic.class));

    HelixVeniceClusterResources veniceClusterResources = mock(HelixVeniceClusterResources.class);
    doReturn(veniceClusterResources).when(veniceHelixAdmin).getHelixVeniceClusterResources(clusterName);
    ClusterLockManager clusterLockManager = mock(ClusterLockManager.class);
    when(veniceClusterResources.getClusterLockManager()).thenReturn(clusterLockManager);

    // Case 4: Store exists, it's a user system store, first check if real-time topic exists returns false but
    // later RT topic was created
    topicManager = mock(TopicManager.class);
    doReturn(topicManager).when(veniceHelixAdmin).getTopicManager();
    doReturn(false).doReturn(true).when(topicManager).containsTopic(any(PubSubTopic.class));
    veniceHelixAdmin.ensureRealTimeTopicExistsForUserSystemStores(clusterName, systemStoreName);
    verify(topicManager, times(2)).containsTopic(any(PubSubTopic.class));
    verify(topicManager, never()).createTopic(
        any(PubSubTopic.class),
        anyInt(),
        anyInt(),
        anyLong(),
        anyBoolean(),
        any(Optional.class),
        anyBoolean());

    // Case 5: Store exists, it's a user system store, but real-time topic does not exist and there are no versions
    // and store partition count is zero
    doReturn(0).when(systemStore).getPartitionCount();
    doReturn(false).when(topicManager).containsTopic(any(PubSubTopic.class));
    Exception zeroPartitionCountException = expectThrows(
        VeniceException.class,
        () -> veniceHelixAdmin.ensureRealTimeTopicExistsForUserSystemStores(clusterName, systemStoreName));
    assertTrue(
        zeroPartitionCountException.getMessage().contains("partition count set to 0"),
        "Actual message: " + zeroPartitionCountException.getMessage());

    // Case 6: Store exists, it's a user system store, but real-time topic does not exist and there are no versions
    // hence create a new real-time topic should use store's partition count

    doReturn(false).when(topicManager).containsTopic(any(PubSubTopic.class));
    doReturn(null).when(systemStore).getVersion(anyInt());
    doReturn(5).when(systemStore).getPartitionCount();
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(veniceHelixAdmin.getControllerConfig(clusterName)).thenReturn(clusterConfig);
    veniceHelixAdmin.ensureRealTimeTopicExistsForUserSystemStores(clusterName, systemStoreName);
    ArgumentCaptor<Integer> partitionCountArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(topicManager, times(1)).createTopic(
        any(PubSubTopic.class),
        partitionCountArgumentCaptor.capture(),
        anyInt(),
        anyLong(),
        anyBoolean(),
        any(Optional.class),
        anyBoolean());
    assertEquals(partitionCountArgumentCaptor.getValue().intValue(), 5);

    // Case 7: Store exists, it's a user system store, but real-time topic does not exist and there are versions
    version = mock(Version.class);
    topicManager = mock(TopicManager.class);
    doReturn(topicManager).when(veniceHelixAdmin).getTopicManager();
    doReturn(false).when(topicManager).containsTopic(any(PubSubTopic.class));
    doReturn(version).when(systemStore).getVersion(anyInt());
    doReturn(10).when(version).getPartitionCount();
    veniceHelixAdmin.ensureRealTimeTopicExistsForUserSystemStores(clusterName, systemStoreName);
    partitionCountArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(topicManager, times(1)).createTopic(
        any(PubSubTopic.class),
        partitionCountArgumentCaptor.capture(),
        anyInt(),
        anyLong(),
        anyBoolean(),
        any(Optional.class),
        anyBoolean());
    assertEquals(partitionCountArgumentCaptor.getValue().intValue(), 10);
  }
}
