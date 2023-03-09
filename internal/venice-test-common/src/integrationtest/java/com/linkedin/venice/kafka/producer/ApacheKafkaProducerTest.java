package com.linkedin.venice.kafka.producer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_LINGER_MS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_VALUE_SERIALIZER;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.SimplePubSubProducerCallbackImpl;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapter;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.Utils;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests specific to Kafka Producer.
 * Important Notes:
 * 1) Please use this class to verify the guarantees expected from Kafka producer by Venice.
 * 2) Do NOT use this class for testing general venice code paths.
 * 3) Whenever possible tests should be against {@link ApacheKafkaProducerAdapter} and not {@link org.apache.kafka.clients.producer.KafkaProducer}
 */
public class ApacheKafkaProducerTest {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerTest.class);

  private ZkServerWrapper zkServerWrapper;
  private KafkaBrokerWrapper kafkaBrokerWrapper;
  // todo: AdminClient should be replaced with KafkaAdminClientAdapter when it is available
  private AdminClient kafkaAdminClient;

  private ApacheKafkaProducerAdapter kafkaProducerAdapter;

  @BeforeClass(alwaysRun = true)
  public void setupKafka() {
    zkServerWrapper = ServiceFactory.getZkServer();
    kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServerWrapper);
    Properties kafkaAdminProperties = new Properties();
    kafkaAdminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerWrapper.getAddress());
    kafkaAdminClient = KafkaAdminClient.create(kafkaAdminProperties);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    kafkaAdminClient.close(Duration.ZERO);
    kafkaBrokerWrapper.close();
    zkServerWrapper.close();
  }

  private void createTopic(
      String topicName,
      int numPartitions,
      int replicationFactor,
      Map<String, String> topicPropertiesMap) {
    NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor).configs(topicPropertiesMap);
    CreateTopicsResult createTopicsResult = kafkaAdminClient.createTopics(Collections.singleton(newTopic));
    try {
      createTopicsResult.all().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Failed to create topic: {}", topicName, e);
      throw new RuntimeException(e);
    }
  }

  private KafkaKey getDummyKey() {
    return new KafkaKey(MessageType.PUT, Utils.getUniqueString("key-").getBytes());
  }

  @Test
  public void testProducerUngracefulCloseFromCallback() throws ExecutionException, InterruptedException {
    String topicName = Utils.getUniqueString("test-topic-for-callback-close-test");
    createTopic(
        topicName,
        1,
        1,
        Collections.singletonMap(TopicConfig.RETENTION_MS_CONFIG, Long.toString(Long.MAX_VALUE)));
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress());
    properties.put(KAFKA_LINGER_MS, 0);
    properties.put(KAFKA_VALUE_SERIALIZER, ByteArraySerializer.class.getName());
    ApacheKafkaProducerConfig producerConfig = new ApacheKafkaProducerConfig(properties, false);
    ApacheKafkaProducerAdapter producer = new ApacheKafkaProducerAdapter(producerConfig);

    // case 1: block and drop messages in accumulator
    KafkaKey m0Key = getDummyKey();
    SimpleBlockingCallback m0BlockingCallback = new SimpleBlockingCallback(m0Key);
    Future<PubSubProduceResult> m0Future = producer.sendMessage(topicName, null, m0Key, null, null, m0BlockingCallback);

    KafkaKey m1Key = getDummyKey();
    SimplePubSubProducerCallbackImpl m1SimpleCallback = new SimplePubSubProducerCallbackImpl();
    Future<PubSubProduceResult> m1Future = producer.sendMessage(topicName, null, m1Key, null, null, m1SimpleCallback);

    // wait until producer's sender(ioThread) is blocked
    m0BlockingCallback.mutex.lock();
    try {
      while (!m0BlockingCallback.blockedSuccessfully) {
        m0BlockingCallback.blockedSuccessfullyCv.await();
      }
    } finally {
      m0BlockingCallback.mutex.unlock();
    }
    // if control reaches here, it means producer's sender thread is blocked
    assertTrue(m0Future.isDone()); // verify, and then trust!
    assertFalse(m1Future.isDone());

    // let's add some records (m1 to m99) to producer's buffer/accumulator
    Map<SimplePubSubProducerCallbackImpl, Future<PubSubProduceResult>> produceResults = new LinkedHashMap<>(100);
    for (int i = 0; i < 100; i++) {
      SimplePubSubProducerCallbackImpl callback = new SimplePubSubProducerCallbackImpl();
      produceResults.put(callback, producer.sendMessage(topicName, null, getDummyKey(), null, null, callback));
    }

    // let's make sure that none of the m1 to m99 are ACKed by Kafka yet
    produceResults.forEach((cb, future) -> {
      assertFalse(cb.isInvoked());
      assertFalse(future.isDone());
    });

    // producer.cl; // close producer immediately

    System.out.println(m0BlockingCallback + " - " + m0Future);
    produceResults.forEach((key, value) -> System.out.println(key + " - " + value));

    //
    // try {
    // future.get();
    // } catch (Exception e) {
    // LOGGER.error(e);
    // }
    //
    // LOGGER.info("##CB##: {} invoked: {}", pubSubProducerCallback, pubSubProducerCallback.isInvoked());
    // System.out.println("hello");

    // case 2: ignore messages sent

  }

  static class SimpleBlockingCallback implements PubSubProducerCallback {
    boolean block = true;
    boolean blockedSuccessfully = false;
    KafkaKey kafkaKey;

    Lock mutex = new ReentrantLock();
    Condition blockCv = mutex.newCondition();
    Condition blockedSuccessfullyCv = mutex.newCondition();

    SimpleBlockingCallback(KafkaKey kafkaKey) {
      this.kafkaKey = kafkaKey;
    }

    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
      LOGGER.info("Blocking callback invoked for key: {}. Executing thread will be blocked", kafkaKey);
      mutex.lock();
      try {
        while (block) {
          blockedSuccessfully = true;
          blockedSuccessfullyCv.signal();
          blockCv.await();
        }
      } catch (InterruptedException e) {
        LOGGER.error("Something went wrong while waiting to receive unblock signal in blocking callback", e);
      } finally {
        mutex.unlock();
      }
      LOGGER.info("Blocking callback invoked for key:{} has unblocked executing thread", kafkaKey);
    }
  }

  // @Test(timeOut = 30000)
  // public void testProducerCloses() {
  // String topicName = Utils.getUniqueString("topic-for-vw-thread-safety");
  // int partitionCount = 1;
  // createTopic(topicName, 1, 1, Collections.singletonMap(TopicConfig.RETENTION_MS_CONFIG,
  // Long.toString(Long.MAX_VALUE)));
  // Properties properties = new Properties();
  // properties.put(ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS, kafka.getAddress());
  // PubSubProducerAdapter producer = veniceWriter.getProducerAdapter();
  //
  //
  // ExecutorService executor = Executors.newSingleThreadExecutor();
  // CountDownLatch countDownLatch = new CountDownLatch(1);
  //
  // try {
  // Future future = executor.submit(() -> {
  // countDownLatch.countDown();
  // // send to non-existent topic
  // producer.sendMessage("topic", null, new KafkaKey(MessageType.PUT, "key".getBytes()), null, null, null);
  // fail("Should be blocking send");
  // });
  //
  // try {
  // countDownLatch.await();
  // // Still wait for some time to make sure blocking sendMessage is inside kafka before closing it.
  // Utils.sleep(50);
  // producer.close(5000, true);
  // } catch (Exception e) {
  // fail("Close should be able to close.", e);
  // }
  // try {
  // future.get();
  // } catch (ExecutionException exception) {
  // assertEquals(
  // exception.getCause().getMessage(),
  // "Got an error while trying to produce message into Kafka. Topic: 'topic', partition: null");
  // } catch (Exception e) {
  // fail(" Should not throw other types of exception", e);
  // }
  // } finally {
  // executor.shutdownNow();
  // }
  // }

}
