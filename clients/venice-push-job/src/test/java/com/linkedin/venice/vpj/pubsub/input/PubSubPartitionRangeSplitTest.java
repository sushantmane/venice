package com.linkedin.venice.vpj.pubsub.input;

import static org.testng.Assert.*;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * TestNG unit tests for {@link PubSubPartitionRangeSplit}.
 * Tests constructor behavior, getter methods, and various split scenarios
 * following Java 8 compatibility requirements.
 */
public class PubSubPartitionRangeSplitTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private PubSubTopic testTopic;
  private PubSubTopicPartition testPartition;

  @BeforeMethod
  public void setUp() {
    testTopic = TOPIC_REPOSITORY.getTopic("test-topic_v1");
    testPartition = new PubSubTopicPartitionImpl(testTopic, 0);
  }

  @Test
  public void testBasicConstructorAndGetters() {
    // Case 1: Standard range split construction
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(200);
    long recordCount = 100;
    int rangeIndex = 0;

    PubSubPartitionRangeSplit split = createSplit(testPartition, startPos, endPos, recordCount, rangeIndex);

    assertEquals(split.getPubSubTopicPartition(), testPartition);
    assertEquals(split.getStartPubSubPosition(), startPos);
    assertEquals(split.getEndPubSubPosition(), endPos);
    assertEquals(split.getNumberOfRecords(), recordCount);
    assertEquals(split.getRangeIndex(), rangeIndex);

    // Case 2: Single record range
    startPos = ApacheKafkaOffsetPosition.of(50);
    endPos = ApacheKafkaOffsetPosition.of(51);
    recordCount = 1;
    rangeIndex = 5;

    split = createSplit(testPartition, startPos, endPos, recordCount, rangeIndex);

    assertEquals(split.getPubSubTopicPartition(), testPartition);
    assertEquals(split.getStartPubSubPosition(), startPos);
    assertEquals(split.getEndPubSubPosition(), endPos);
    assertEquals(split.getNumberOfRecords(), recordCount);
    assertEquals(split.getRangeIndex(), rangeIndex);

    // Case 3: Large record count with high range index
    startPos = ApacheKafkaOffsetPosition.of(1000000);
    endPos = ApacheKafkaOffsetPosition.of(2000000);
    recordCount = 1000000L;
    rangeIndex = 99;

    split = createSplit(testPartition, startPos, endPos, recordCount, rangeIndex);

    assertEquals(split.getPubSubTopicPartition(), testPartition);
    assertEquals(split.getStartPubSubPosition(), startPos);
    assertEquals(split.getEndPubSubPosition(), endPos);
    assertEquals(split.getNumberOfRecords(), recordCount);
    assertEquals(split.getRangeIndex(), rangeIndex);
  }

  @Test
  public void testEdgeCaseScenarios() {
    // Case 1: Zero records (empty range)
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPartitionRangeSplit split = createSplit(testPartition, startPos, endPos, 0, 0);

    assertEquals(split.getNumberOfRecords(), 0);
    assertEquals(split.getStartPubSubPosition(), startPos);
    assertEquals(split.getEndPubSubPosition(), endPos);
    assertEquals(split.getRangeIndex(), 0);

    // Case 2: Range starting at offset zero
    startPos = ApacheKafkaOffsetPosition.of(0);
    endPos = ApacheKafkaOffsetPosition.of(50);
    split = createSplit(testPartition, startPos, endPos, 50, 0);

    assertEquals(split.getStartPubSubPosition(), startPos);
    assertEquals(split.getEndPubSubPosition(), endPos);
    assertEquals(split.getNumberOfRecords(), 50);
    assertEquals(split.getRangeIndex(), 0);

    // Case 3: Consecutive positions (single record)
    startPos = ApacheKafkaOffsetPosition.of(999);
    endPos = ApacheKafkaOffsetPosition.of(1000);
    split = createSplit(testPartition, startPos, endPos, 1, 10);

    assertEquals(split.getStartPubSubPosition(), startPos);
    assertEquals(split.getEndPubSubPosition(), endPos);
    assertEquals(split.getNumberOfRecords(), 1);
    assertEquals(split.getRangeIndex(), 10);
  }

  @Test(dataProvider = "rangeIndexScenarios")
  public void testRangeIndexProgression(int[] rangeIndexes, long[] recordCounts) {
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(0);

    for (int i = 0; i < rangeIndexes.length; i++) {
      long offset = i * 100;
      PubSubPosition currentStart = ApacheKafkaOffsetPosition.of(offset);
      PubSubPosition currentEnd = ApacheKafkaOffsetPosition.of(offset + recordCounts[i]);

      PubSubPartitionRangeSplit split =
          createSplit(testPartition, currentStart, currentEnd, recordCounts[i], rangeIndexes[i]);

      assertEquals(split.getRangeIndex(), rangeIndexes[i], "Range index should match for split " + i);
      assertEquals(split.getNumberOfRecords(), recordCounts[i], "Record count should match for split " + i);
      assertEquals(split.getStartPubSubPosition(), currentStart, "Start position should match for split " + i);
      assertEquals(split.getEndPubSubPosition(), currentEnd, "End position should match for split " + i);
    }
  }

  @Test
  public void testPartitionConsistency() {
    // Case 1: Same partition across multiple splits
    PubSubPartitionRangeSplit split1 =
        createSplit(testPartition, ApacheKafkaOffsetPosition.of(0), ApacheKafkaOffsetPosition.of(50), 50, 0);
    PubSubPartitionRangeSplit split2 =
        createSplit(testPartition, ApacheKafkaOffsetPosition.of(50), ApacheKafkaOffsetPosition.of(100), 50, 1);

    assertEquals(split1.getPubSubTopicPartition(), split2.getPubSubTopicPartition());
    assertNotEquals(split1.getRangeIndex(), split2.getRangeIndex());

    // Case 2: Different partitions
    PubSubTopicPartition partition1 = new PubSubTopicPartitionImpl(testTopic, 1);
    PubSubTopicPartition partition2 = new PubSubTopicPartitionImpl(testTopic, 2);

    split1 = createSplit(partition1, ApacheKafkaOffsetPosition.of(0), ApacheKafkaOffsetPosition.of(100), 100, 0);
    split2 = createSplit(partition2, ApacheKafkaOffsetPosition.of(0), ApacheKafkaOffsetPosition.of(100), 100, 0);

    assertNotEquals(split1.getPubSubTopicPartition(), split2.getPubSubTopicPartition());
    assertEquals(split1.getRangeIndex(), split2.getRangeIndex()); // Same range index for different partitions is
                                                                  // allowed
  }

  @Test
  public void testImmutabilityBehavior() {
    // Case 1: Verify objects retain their values after construction
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(500);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(1000);
    long recordCount = 500;
    int rangeIndex = 3;

    PubSubPartitionRangeSplit split = createSplit(testPartition, startPos, endPos, recordCount, rangeIndex);

    // Store original values
    PubSubTopicPartition originalPartition = split.getPubSubTopicPartition();
    PubSubPosition originalStart = split.getStartPubSubPosition();
    PubSubPosition originalEnd = split.getEndPubSubPosition();
    long originalRecords = split.getNumberOfRecords();
    int originalIndex = split.getRangeIndex();

    // Multiple calls should return same values
    assertEquals(split.getPubSubTopicPartition(), originalPartition);
    assertEquals(split.getStartPubSubPosition(), originalStart);
    assertEquals(split.getEndPubSubPosition(), originalEnd);
    assertEquals(split.getNumberOfRecords(), originalRecords);
    assertEquals(split.getRangeIndex(), originalIndex);

    // Case 2: Different instances with same values should have same getter results
    PubSubPartitionRangeSplit split2 = createSplit(testPartition, startPos, endPos, recordCount, rangeIndex);

    assertEquals(split.getPubSubTopicPartition(), split2.getPubSubTopicPartition());
    assertEquals(split.getStartPubSubPosition(), split2.getStartPubSubPosition());
    assertEquals(split.getEndPubSubPosition(), split2.getEndPubSubPosition());
    assertEquals(split.getNumberOfRecords(), split2.getNumberOfRecords());
    assertEquals(split.getRangeIndex(), split2.getRangeIndex());
  }

  @DataProvider(name = "rangeIndexScenarios")
  public Object[][] rangeIndexScenarios() {
    return new Object[][] {
        // rangeIndexes, recordCounts
        { new int[] { 0, 1, 2, 3, 4 }, new long[] { 100, 200, 150, 300, 50 } }, { new int[] { 0 }, new long[] { 1 } },
        { new int[] { 0, 1 }, new long[] { 0, 1000 } }, { new int[] { 10, 11, 12 }, new long[] { 500, 500, 500 } },
        { new int[] { 0, 5, 10, 15, 20 }, new long[] { 1, 1, 1, 1, 1 } } };
  }

  private PubSubPartitionRangeSplit createSplit(
      PubSubTopicPartition partition,
      PubSubPosition startPos,
      PubSubPosition endPos,
      long recordCount,
      int rangeIndex) {
    return new PubSubPartitionRangeSplit(partition, startPos, endPos, recordCount, rangeIndex);
  }
}
