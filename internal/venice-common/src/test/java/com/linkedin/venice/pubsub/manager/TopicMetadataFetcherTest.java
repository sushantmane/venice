package com.linkedin.venice.pubsub.manager;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.manager.TopicMetadataFetcher.ValueAndExpiryTime;
import com.linkedin.venice.utils.ExceptionUtils;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMaps;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TopicMetadataFetcherTest {
  private TopicMetadataFetcher topicMetadataFetcher;
  private TopicManagerContext topicManagerContext;
  private String pubSubClusterAddress = "venicedb.pubsub.standalone:9092";
  private TopicManagerStats topicManagerStats;
  private PubSubAdminAdapter adminMock;
  private BlockingQueue<PubSubConsumerAdapter> pubSubConsumerPool;
  private ThreadPoolExecutor threadPoolExecutor;
  private long cachedEntryTtlInNs = TimeUnit.MINUTES.toNanos(5);
  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private String topicName = "testTopicName";
  private PubSubTopic pubSubTopic;
  private PubSubConsumerAdapter consumerMock;

  @BeforeMethod
  public void setUp() throws InterruptedException {
    consumerMock = mock(PubSubConsumerAdapter.class);
    pubSubConsumerPool = new LinkedBlockingQueue<>(2);
    pubSubConsumerPool.put(consumerMock);
    threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
    adminMock = mock(PubSubAdminAdapter.class);
    pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    topicMetadataFetcher = new TopicMetadataFetcher(
        pubSubClusterAddress,
        topicManagerContext,
        topicManagerStats,
        adminMock,
        pubSubConsumerPool,
        threadPoolExecutor,
        cachedEntryTtlInNs);
  }

  @Test
  public void testClose() {
    assertEquals(pubSubConsumerPool.size(), 1);

    CountDownLatch signalReceiver = new CountDownLatch(1);
    List<PubSubTopicPartitionInfo> partitions = new ArrayList<>();
    CountDownLatch holdConsumer = new CountDownLatch(1);
    doAnswer(invocation -> {
      try {
        signalReceiver.countDown();
        return partitions;
      } catch (Exception e) {
        throw e;
      } finally {
        holdConsumer.await();
      }
    }).when(consumerMock).partitionsFor(pubSubTopic);

    CompletableFuture<List<PubSubTopicPartitionInfo>> future = CompletableFuture
        .supplyAsync(() -> topicMetadataFetcher.getTopicPartitionInfo(pubSubTopic), threadPoolExecutor);
    try {
      signalReceiver.await(3, TimeUnit.MINUTES);
    } catch (Exception e) {
      fail("Timed out waiting for signalReceiver");
    }

    try {
      topicMetadataFetcher.close();
    } catch (Exception e) {
      fail("TopicMetadataFetcher::close should not throw exception when closing");
    }
    Throwable t = expectThrows(ExecutionException.class, future::get);
    assertTrue(ExceptionUtils.recursiveClassEquals(t, InterruptedException.class));
    verify(consumerMock, times(1)).partitionsFor(pubSubTopic);
    verify(consumerMock, times(1)).close();
  }

  @Test
  public void testValidateTopicPartition() {
    assertThrows(NullPointerException.class, () -> topicMetadataFetcher.validateTopicPartition(null));

    final PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topicName), -1);
    assertThrows(IllegalArgumentException.class, () -> topicMetadataFetcher.validateTopicPartition(tp1));

    final PubSubTopicPartition tp2 = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topicName), 0);
    TopicMetadataFetcher topicMetadataFetcherSpy = spy(topicMetadataFetcher);
    doReturn(false).when(topicMetadataFetcherSpy).containsTopicCached(tp2.getPubSubTopic());
    Exception e =
        expectThrows(PubSubTopicDoesNotExistException.class, () -> topicMetadataFetcherSpy.validateTopicPartition(tp2));
    assertTrue(e.getMessage().contains("does not exist"));

    doReturn(true).when(topicMetadataFetcherSpy).containsTopicCached(tp2.getPubSubTopic());
    topicMetadataFetcherSpy.validateTopicPartition(tp2);

    verify(topicMetadataFetcherSpy, times(2)).containsTopicCached(tp2.getPubSubTopic());
  }

  @Test
  public void testContainsTopic() {
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(false);
    assertFalse(topicMetadataFetcher.containsTopic(pubSubTopic));

    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);
    assertTrue(topicMetadataFetcher.containsTopic(pubSubTopic));

    verify(adminMock, times(2)).containsTopic(pubSubTopic);
  }

  @Test
  public void testContainsTopicAsync() {
    when(adminMock.containsTopic(pubSubTopic)).thenReturn(false);
    assertFalse(topicMetadataFetcher.containsTopicAsync(pubSubTopic).join());

    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);
    assertTrue(topicMetadataFetcher.containsTopicAsync(pubSubTopic).join());

    doThrow(new PubSubClientException("Test")).when(adminMock).containsTopic(pubSubTopic);
    CompletableFuture<Boolean> future = topicMetadataFetcher.containsTopicAsync(pubSubTopic);
    ExecutionException e = expectThrows(ExecutionException.class, future::get);
    assertTrue(ExceptionUtils.recursiveClassEquals(e, PubSubClientException.class));

    verify(adminMock, times(3)).containsTopic(pubSubTopic);
  }

  @Test
  public void testUpdateCacheAsyncWhenCachedValueIsNotStaleOrWhenUpdateIsInProgressShouldNotUpdateCache() {
    // Set the cached value to be not stale and update not in progress
    Supplier<CompletableFuture<Boolean>> cfSupplierMock = mock(Supplier.class);
    Map<PubSubTopic, ValueAndExpiryTime<Boolean>> cache = new ConcurrentHashMap<>();
    ValueAndExpiryTime<Boolean> cachedValue = topicMetadataFetcher.new ValueAndExpiryTime<>(true);
    cache.put(pubSubTopic, cachedValue);
    topicMetadataFetcher.updateCacheAsync(pubSubTopic, cachedValue, cache, cfSupplierMock);
    verify(cfSupplierMock, never()).get();

    // Set the cached value to be stale and update in progress
    cachedValue.setExpiryTimeNs(System.nanoTime() - 1);
    cachedValue.setUpdateInProgressStatus(true);
    topicMetadataFetcher.updateCacheAsync(pubSubTopic, cachedValue, cache, cfSupplierMock);
    verify(cfSupplierMock, never()).get();
  }

  @Test
  public void testUpdateCacheAsync() {
    // WhenCachedValueIsNull --> ShouldUpdateCache
    Supplier<CompletableFuture<Boolean>> cfSupplierMock = mock(Supplier.class);
    when(cfSupplierMock.get()).thenReturn(CompletableFuture.completedFuture(true));
    Map<PubSubTopic, ValueAndExpiryTime<Boolean>> cache = new ConcurrentHashMap<>();
    topicMetadataFetcher.updateCacheAsync(pubSubTopic, null, cache, cfSupplierMock);
    assertEquals(cache.size(), 1);
    assertTrue(cache.containsKey(pubSubTopic));
    // if we can acquire the lock, it means it was released after the update
    assertTrue(cache.get(pubSubTopic).tryAcquireUpdateLock());
    verify(cfSupplierMock, times(1)).get();

    // WhenCachedValueIsStaleAndWhenAsyncUpdateSucceeds --> ShouldUpdateCache
    ValueAndExpiryTime<Boolean> cachedValue = topicMetadataFetcher.new ValueAndExpiryTime<>(true);
    cachedValue.setExpiryTimeNs(System.nanoTime() - 1);
    topicMetadataFetcher.updateCacheAsync(pubSubTopic, cachedValue, cache, cfSupplierMock);
    assertEquals(cache.size(), 1);
    assertTrue(cache.containsKey(pubSubTopic));
    // if we can acquire the lock, it means it was released after the update
    assertTrue(cache.get(pubSubTopic).tryAcquireUpdateLock());
    assertTrue(cache.get(pubSubTopic).getValue());
    verify(cfSupplierMock, times(2)).get();

    // WhenAsyncUpdateFails --> ShouldRemoveFromCache
    cache.remove(pubSubTopic);
    CompletableFuture<Boolean> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new PubSubClientException("Test"));
    when(cfSupplierMock.get()).thenReturn(failedFuture);
    topicMetadataFetcher.updateCacheAsync(pubSubTopic, null, cache, cfSupplierMock);
    assertEquals(cache.size(), 0);
    assertFalse(cache.containsKey(pubSubTopic));
    verify(cfSupplierMock, times(3)).get();
  }

  @Test
  public void testGetTopicLatestOffsets() {
    assertEquals(pubSubConsumerPool.size(), 1);
    // test consumer::partitionFor --> (null, empty list)
    when(consumerMock.partitionsFor(pubSubTopic)).thenReturn(null).thenReturn(Collections.emptyList());
    for (int i = 0; i < 2; i++) {
      verify(consumerMock, times(i)).partitionsFor(pubSubTopic);
      Int2LongMap res = topicMetadataFetcher.getTopicLatestOffsets(pubSubTopic);
      assertEquals(res, Int2LongMaps.EMPTY_MAP);
      assertEquals(res.size(), 0);
      verify(consumerMock, times(i + 1)).partitionsFor(pubSubTopic);
    }

    // test consumer::partitionFor returns non-empty list
    PubSubTopicPartitionInfo tp0Info = new PubSubTopicPartitionInfo(pubSubTopic, 0, true);
    PubSubTopicPartitionInfo tp1Info = new PubSubTopicPartitionInfo(pubSubTopic, 1, true);
    List<PubSubTopicPartitionInfo> partitionInfo = Arrays.asList(tp0Info, tp1Info);
    Map<PubSubTopicPartition, Long> offsetsMap = new ConcurrentHashMap<>();
    offsetsMap.put(tp0Info.getTopicPartition(), 111L);
    offsetsMap.put(tp1Info.getTopicPartition(), 222L);

    when(consumerMock.partitionsFor(pubSubTopic)).thenReturn(partitionInfo);
    when(consumerMock.endOffsets(eq(offsetsMap.keySet()), any(Duration.class))).thenReturn(offsetsMap);

    Int2LongMap res = topicMetadataFetcher.getTopicLatestOffsets(pubSubTopic);
    assertEquals(res.size(), offsetsMap.size());
    assertEquals(res.get(0), 111L);
    assertEquals(res.get(1), 222L);

    verify(consumerMock, times(3)).partitionsFor(pubSubTopic);
    verify(consumerMock, times(1)).endOffsets(eq(offsetsMap.keySet()), any(Duration.class));

    // check if consumer was released back to the pool
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testGetTopicPartitionInfo() {
    assertEquals(pubSubConsumerPool.size(), 1);
    PubSubTopicPartitionInfo tp0Info = new PubSubTopicPartitionInfo(pubSubTopic, 0, true);
    PubSubTopicPartitionInfo tp1Info = new PubSubTopicPartitionInfo(pubSubTopic, 1, true);
    List<PubSubTopicPartitionInfo> partitionInfo = Arrays.asList(tp0Info, tp1Info);
    when(consumerMock.partitionsFor(pubSubTopic)).thenReturn(partitionInfo);
    List<PubSubTopicPartitionInfo> res = topicMetadataFetcher.getTopicPartitionInfo(pubSubTopic);
    assertEquals(res.size(), partitionInfo.size());
    assertEquals(res.get(0), tp0Info);
    assertEquals(res.get(1), tp1Info);
    assertEquals(pubSubConsumerPool.size(), 1);
  }

  @Test
  public void testConsumeLatestRecords() {
    PubSubTopicPartition invalidTp = new PubSubTopicPartitionImpl(pubSubTopic, -1);
    Throwable t =
        expectThrows(IllegalArgumentException.class, () -> topicMetadataFetcher.consumeLatestRecords(invalidTp, 1));
    assertTrue(t.getMessage().contains("Invalid partition number"));

    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    t = expectThrows(
        IllegalArgumentException.class,
        () -> topicMetadataFetcher.consumeLatestRecords(topicPartition, 0));
    assertTrue(t.getMessage().contains("Last record count must be greater than or equal to 1."));

    when(adminMock.containsTopic(pubSubTopic)).thenReturn(true);

    when(consumerMock.endOffsets(eq(Collections.singleton(topicPartition)), any(Duration.class))).thenReturn(null);
    t = expectThrows(VeniceException.class, () -> topicMetadataFetcher.consumeLatestRecords(topicPartition, 1));
    assertTrue(t.getMessage().contains("Failed to get the end offset for topic-partition:"));

    when(consumerMock.endOffsets(eq(Collections.singleton(topicPartition)), any(Duration.class)))
        .thenReturn(Collections.emptyMap());
    t = expectThrows(VeniceException.class, () -> topicMetadataFetcher.consumeLatestRecords(topicPartition, 1));
    assertTrue(t.getMessage().contains("Failed to get the end offset for topic-partition:"));

    // no records to consume as endOffset is 0
    Map<PubSubTopicPartition, Long> offsetMap = new HashMap<>();
    offsetMap.put(topicPartition, 0L);
    when(consumerMock.endOffsets(eq(Collections.singletonList(topicPartition)), any(Duration.class)))
        .thenReturn(offsetMap);
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumedRecords =
        topicMetadataFetcher.consumeLatestRecords(topicPartition, 1);
    assertEquals(consumedRecords.size(), 0);

    // beginningOffset (non-zero) is same as endOffset
    offsetMap.put(topicPartition, 1L);
    when(consumerMock.endOffsets(eq(Collections.singletonList(topicPartition)), any(Duration.class)))
        .thenReturn(offsetMap);
    when(consumerMock.beginningOffset(eq(topicPartition), any(Duration.class))).thenReturn(1L);
    consumedRecords = topicMetadataFetcher.consumeLatestRecords(topicPartition, 1);
    assertEquals(consumedRecords.size(), 0);

  }

  @Test
  public void testGetProducerTimestampOfLastDataMessage() {

  }

}
