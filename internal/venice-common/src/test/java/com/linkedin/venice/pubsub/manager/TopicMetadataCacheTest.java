package com.linkedin.venice.pubsub.manager;

import static org.mockito.ArgumentMatchers.*;


public class TopicMetadataCacheTest {
  // private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  // @Test
  // public void testGetEarliestOffset() {
  // TopicMetadataCache topicMetadataCache = new TopicMetadataCache(1000);
  // TopicManager mockTopicManager = mock(TopicManager.class);
  // PubSubTopic testTopic = pubSubTopicRepository.getTopic("test_v1");
  // int partition = 0;
  // PubSubTopicPartition testTopicPartition = new PubSubTopicPartitionImpl(testTopic, partition);
  // String testBrokerUrl = "I_Am_A_Broker_dot_com.com";
  // Long earliestOffset = 1L;
  // when(mockTopicManager.getPubSubClusterAddress()).thenReturn(testBrokerUrl);
  // when(mockTopicManager.getPartitionEarliestOffsetWithRetries(any(), anyInt())).thenReturn(earliestOffset);
  // Assert.assertEquals(
  // (Long) topicMetadataCache.getEarliestOffset(mockTopicManager, testTopicPartition),
  // earliestOffset);
  //
  // TopicManager mockTopicManagerThatThrowsException = mock(TopicManager.class);
  // when(mockTopicManagerThatThrowsException.getPubSubClusterAddress()).thenReturn(testBrokerUrl);
  // when(mockTopicManagerThatThrowsException.getPartitionEarliestOffsetWithRetries(any(), anyInt()))
  // .thenThrow(PubSubTopicDoesNotExistException.class);
  //
  // // Even though we're passing a weird topic manager, we should have cached the last value, so this should return the
  // // cached offset of 1
  // Assert.assertEquals(
  // (Long) topicMetadataCache.getEarliestOffset(mockTopicManagerThatThrowsException, testTopicPartition),
  // earliestOffset);
  //
  // // Now check for an uncached value and verify we get the error code for topic does not exist.
  // Assert.assertEquals(
  // topicMetadataCache.getEarliestOffset(
  // mockTopicManagerThatThrowsException,
  // new PubSubTopicPartitionImpl(testTopic, partition + 1)),
  // StatsErrorCode.LAG_MEASUREMENT_FAILURE.code);
  //
  // }
  //
  // @Test
  // public void testCacheWillResetStatusWhenExceptionIsThrown() {
  // TopicMetadataCache topicMetadataCache = new TopicMetadataCache(1000);
  // TopicMetadataCache.PubSubMetadataCacheKey key = new TopicMetadataCache.PubSubMetadataCacheKey(
  // "server",
  // new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic")), 1));
  // Map<TopicMetadataCache.PubSubMetadataCacheKey, TopicMetadataCache.ValueAndExpiryTime<Long>> offsetCache =
  // new VeniceConcurrentHashMap<>();
  // TopicMetadataCache.ValueAndExpiryTime<Long> valueCache =
  // new TopicMetadataCache.ValueAndExpiryTime<>(1L, System.nanoTime());
  // offsetCache.put(key, valueCache);
  // // Successful call will update the value from 1 to 2.
  // TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
  // Long actualResult = topicMetadataCache.fetchMetadata(key, offsetCache, () -> 2L);
  // Long expectedResult = 2L;
  // Assert.assertEquals(actualResult, expectedResult);
  // });
  //
  // // For persisting exception, it will be caught and thrown.
  // TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
  // Assert
  // .assertThrows(VeniceException.class, () -> topicMetadataCache.fetchMetadata(key, offsetCache, () -> {
  // throw new VeniceException("dummy exception");
  // }));
  // });
  //
  // // Reset the cached value to 1.
  // valueCache = new TopicMetadataCache.ValueAndExpiryTime<>(1L, System.nanoTime());
  // offsetCache.put(key, valueCache);
  //
  // // The first call throws a transient exception, and it should be updated to expected value after second call.
  // AtomicBoolean exceptionFlag = new AtomicBoolean(false);
  // TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, false, true, () -> {
  // Long actualResult = topicMetadataCache.fetchMetadata(key, offsetCache, () -> {
  // if (exceptionFlag.compareAndSet(false, true)) {
  // throw new VeniceException("do not throw this exception!");
  // } else {
  // return 2L;
  // }
  // });
  // Long expectedResult = 2L;
  // Assert.assertEquals(actualResult, expectedResult);
  // });
  // }
}
