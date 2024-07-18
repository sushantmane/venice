package com.linkedin.venice.controller;

import static org.testng.Assert.*;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.writer.VeniceWriterFactory;
import org.testng.annotations.BeforeMethod;


public class ParticipantStoreClientsTest {
  private D2Client d2Client;
  private String clusterDiscoveryD2ServiceName;
  private TopicManagerRepository topicManagerRepository;
  private VeniceWriterFactory veniceWriterFactory;
  private PubSubTopicRepository pubSubTopicRepository;

  @BeforeMethod
  public void setUp() {

  }

  // @Test
  // public void testGetReaderNewClientCreated() {
  // AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> reader =
  // yourClassName.getReader(clusterName);
  //
  // }

}
