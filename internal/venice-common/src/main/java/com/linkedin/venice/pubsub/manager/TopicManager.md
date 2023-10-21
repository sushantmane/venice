# TopicManager

TopicManager is a class that manages the topics in a PubSub cluster.
It is responsible for creating, deleting, and listing topics. It also provides a way to get the topi metadata.


## TopicMetadataFetcher APIs

```java

class TopicMetadataFetcher{

    // API#1
    Int2LongMap getTopicLatestOffsets(PubSubTopic topic);



    // API#2
    List<PubSubTopicPartitionInfo> getTopicPartitionInfo(PubSubTopic topic);



    // API#3
    long getLatestOffset(PubSubTopicPartition pubSubTopicPartition);

    long getLatestOffsetWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries);

    CompletableFuture<Long> getLatestOffsetWithRetriesAsync(PubSubTopicPartition pubSubTopicPartition, int retries);



    // API#4
    long getEarliestOffset(PubSubTopicPartition pubSubTopicPartition);

    long getEarliestOffsetWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries);

    CompletableFuture<Long> getEarliestOffsetWithRetriesAsync(PubSubTopicPartition pubSubTopicPartition, int retries);



    // API#5
    long getOffsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp);
    
    long getOffsetForTimeWithRetries(PubSubTopicPartition pubSubTopicPartition, long timestamp, int retries);


    // API#6



    // API#7
    boolean containsTopic(PubSubTopic topic);
    
    CompletableFuture<Boolean> containsTopicAsync(PubSubTopic topic);
}

```