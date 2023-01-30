package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.hadoop.VenicePushJob.ADDRESS_MAP;
import static com.linkedin.venice.hadoop.VenicePushJob.SYSTEM_TEST_ENV;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.hadoop.VenicePushJob;
import java.io.IOException;
import java.util.Properties;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IndefiniteWaitOneShotStartupCheckStrategy;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class VeniceSystemTest {

  // static {
  // System.setProperty("sun.net.spi.nameservice.nameservers", "127.0.0.1");
  // System.setProperty("sun.net.spi.nameservice.provider.1", "dns,sun");
  // }
  private static final Network dc0Net = Network.builder()
      .driver("bridge")
      .createNetworkCmdModifier(createNetworkCmd -> createNetworkCmd.withName("dc-0"))
      .build();
  private static final GenericContainer<?> zkDocker =
      new GenericContainer<>(DockerImageName.parse("venicedb/zookeeper")).withExposedPorts(2181)
          .withImagePullPolicy(PullPolicy.defaultPolicy())
          // .withNetwork(dc0Net)
          .withNetworkAliases("zookeeper")
          .withCreateContainerCmdModifier(cmd -> cmd.withHostName("zookeeper"))
          .withCreateContainerCmdModifier(cmd -> cmd.withName("zookeeper"));
  private static final GenericContainer<?> kafkaDocker =
      new FixedHostPortGenericContainer<>("venicedb/kafka").withFixedExposedPort(9092, 9092)
          // .withNetwork(dc0Net)
          .withImagePullPolicy(PullPolicy.defaultPolicy())
          .withCreateContainerCmdModifier(cmd -> cmd.withHostName("kafka.vendb"))
          .withCreateContainerCmdModifier(cmd -> cmd.withName("kafka"));

  // private static final GenericContainer<?> kafkaDocker = new
  // GenericContainer<>("venicedb/kafka").withExposedPorts(9092)
  // .withImagePullPolicy(PullPolicy.defaultPolicy())
  // .withCreateContainerCmdModifier(cmd -> cmd.withHostName("kafka"))
  // .withCreateContainerCmdModifier(cmd -> cmd.withName("kafka"))
  // .withNetwork(dc0Net);

  private static final GenericContainer<?> controllerDocker =
      new GenericContainer<>(DockerImageName.parse("venicedb/venice-controller")).withExposedPorts(5555)
          .withImagePullPolicy(PullPolicy.defaultPolicy())
          // .withNetwork(dc0Net)
          .withCreateContainerCmdModifier(cmd -> cmd.withHostName("venice-controller"))
          .withCreateContainerCmdModifier(cmd -> cmd.withName("venice-controller"));

  private static final GenericContainer<?> serverDocker =
      new GenericContainer<>(DockerImageName.parse("venicedb/venice-server"))
          .withImagePullPolicy(PullPolicy.defaultPolicy())
          // .withNetwork(dc0Net)
          .withCreateContainerCmdModifier(cmd -> cmd.withHostName("venice-server"))
          .withCreateContainerCmdModifier(cmd -> cmd.withName("venice-server"));

  private static final GenericContainer<?> routerDocker =
      new GenericContainer<>(DockerImageName.parse("venicedb/venice-router")).withExposedPorts(7777)
          .withImagePullPolicy(PullPolicy.defaultPolicy())
          // .withNetwork(dc0Net)
          .withCreateContainerCmdModifier(cmd -> cmd.withHostName("venice-router"))
          .withCreateContainerCmdModifier(cmd -> cmd.withName("venice-router"));
  private static final GenericContainer<?> clientDocker =
      new GenericContainer<>(DockerImageName.parse("venicedb/venice-client"))
          .withImagePullPolicy(PullPolicy.defaultPolicy())
          .withCreateContainerCmdModifier(cmd -> cmd.withHostName("venice-client"))
          .withCreateContainerCmdModifier(cmd -> cmd.withName("venice-client"))
          // .withNetwork(dc0Net)
          .withStartupCheckStrategy(new IndefiniteWaitOneShotStartupCheckStrategy());

  @BeforeClass
  public void startContainers() {
    zkDocker.setDockerImageName("zookeeper");
    zkDocker.start();
    kafkaDocker.start();
    controllerDocker.start();
    serverDocker.start();
    routerDocker.start();
    // clientDocker.withCommand("tail -f /dev/null &").start();
  }

  @AfterClass
  public void stopContainers() {
    routerDocker.stop();
    serverDocker.stop();
    controllerDocker.stop();
    kafkaDocker.stop();
    zkDocker.stop();
    // clientDocker.stop();
  }

  @Test
  public void testJavaDns() throws Exception {
    String clusterName = "venice-cluster";
    String storeName = "test-store";
    String controllerUrl = "http://" + controllerDocker.getHost() + ":" + controllerDocker.getMappedPort(5555);
    String ksf = "/Users/sumane/venice-docker/venice-client/sample-data/schema/keySchema.avsc";
    String vsf = "/Users/sumane/venice-docker/venice-client/sample-data/schema/valueSchema.avsc";
    String vrf = "/Users/sumane/venice-docker/venice-client/sample-data/batch-push-data/";
    String[] args = { "--new-store", "-u", controllerUrl, "-c", clusterName, "-s", storeName, "-ks", ksf, "-vs", vsf };
    AdminTool.main(args);
    args = new String[] { "--update-store", "-u", controllerUrl, "-c", clusterName, "-s", storeName, "--storage-quota",
        "-1", "--incremental-push-enabled", "true" };
    AdminTool.main(args);

    String kafkaBrokerAddressDc0 = "kafka:9092";
    String kafkaBrokerAddressDc0Host = "localhost:" + kafkaDocker.getMappedPort(9092);

    Properties props = new Properties();
    props.put("venice.urls", controllerUrl);
    props.put("venice.discover.urls", controllerUrl);
    props.put("venice.store.name", storeName);
    props.put("input.path", vrf);
    props.put("key.field", "id");
    props.put("value.field", "name");
    props.put("poll.job.status.interval.ms", "1000");
    props.put("controller.request.retry.attempts", "5");
    props.put("venice.writer.close.timeout.ms", "1000");
    props.put("ssl.trust.store.property.name", "test");
    props.put("ssl.key.store.property.name", "test");
    props.put("ssl.key.password.property.name", "test");
    props.put("push.job.status.upload.enable", "false");
    props.put("ssl.key.store.password.property.name", "test");
    // props.put(SYSTEM_TEST_ENV, "true");
    // props.put(ADDRESS_MAP + kafkaBrokerAddressDc0, kafkaBrokerAddressDc0Host);
    VenicePushJob.runPushJob("VJP_1", props);

    System.out.println("zkPort: " + zkDocker.getMappedPort(2181));
  }

  @Test
  public void runTestWithinTheContainer() throws IOException, InterruptedException {
    System.out.println(zkDocker.getFirstMappedPort());

    clientDocker.withCommand("/bin/bash create_store.sh").start();
    clientDocker.stop();
    System.out.println(zkDocker.getFirstMappedPort());
    clientDocker.withCommand("/bin/bash batch_push_data.sh").start();
    System.out.println(zkDocker.getFirstMappedPort());

    System.out.println(zkDocker.getFirstMappedPort());
  }

  @Test
  public void testContainersCanRun() throws Exception {
    String clusterName = "venice-cluster";
    String storeName = "test-store";
    String controllerUrl = "http://" + controllerDocker.getHost() + ":" + controllerDocker.getMappedPort(5555);
    String ksf = "/Users/sumane/venice-docker/venice-client/sample-data/schema/keySchema.avsc";
    String vsf = "/Users/sumane/venice-docker/venice-client/sample-data/schema/valueSchema.avsc";
    String vrf = "/Users/sumane/venice-docker/venice-client/sample-data/batch-push-data/";
    String[] args = { "--new-store", "-u", controllerUrl, "-c", clusterName, "-s", storeName, "-ks", ksf, "-vs", vsf };
    AdminTool.main(args);
    args = new String[] { "--update-store", "-u", controllerUrl, "-c", clusterName, "-s", storeName, "--storage-quota",
        "-1", "--incremental-push-enabled", "true" };
    AdminTool.main(args);

    String kafkaBrokerAddressDc0 = "kafka:9092";
    String kafkaBrokerAddressDc0Host = "localhost:" + kafkaDocker.getMappedPort(9092);

    Properties props = new Properties();
    props.put("venice.urls", controllerUrl);
    props.put("venice.discover.urls", controllerUrl);
    props.put("venice.store.name", storeName);
    props.put("input.path", vrf);
    props.put("key.field", "id");
    props.put("value.field", "name");
    props.put("poll.job.status.interval.ms", "1000");
    props.put("controller.request.retry.attempts", "5");
    props.put("venice.writer.close.timeout.ms", "1000");
    props.put("ssl.trust.store.property.name", "test");
    props.put("ssl.key.store.property.name", "test");
    props.put("ssl.key.password.property.name", "test");
    props.put("push.job.status.upload.enable", "false");
    props.put("ssl.key.store.password.property.name", "test");
    props.put(SYSTEM_TEST_ENV, "true");
    props.put(ADDRESS_MAP + kafkaBrokerAddressDc0, kafkaBrokerAddressDc0Host);
    VenicePushJob.runPushJob("VJP_1", props);

    System.out.println("zkPort: " + zkDocker.getMappedPort(2181));
  }
}
