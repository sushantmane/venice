package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.writer.VeniceWriterFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ParticipantStoreClientsManagerTest {
  private static final String CLUSTER_DISCOVERY_D2_SERVICE_NAME = "d2://venice-cluster-discovery";
  private static final String CLUSTER_NAME = "testCluster";
  private D2Client d2Client;
  private TopicManagerRepository topicManagerRepository;
  private VeniceWriterFactory veniceWriterFactory;
  private PubSubTopicRepository pubSubTopicRepository;
  private ParticipantStoreClientsManager participantStoreClientsManager;

  @BeforeMethod
  public void setUp() {
    d2Client = mock(D2Client.class);
    topicManagerRepository = mock(TopicManagerRepository.class);
    veniceWriterFactory = mock(VeniceWriterFactory.class);
    pubSubTopicRepository = new PubSubTopicRepository();
    participantStoreClientsManager = new ParticipantStoreClientsManager(
        d2Client,
        CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        topicManagerRepository,
        veniceWriterFactory,
        pubSubTopicRepository);
  }

  @Test
  public void testGetReaderCreatesNewClient() {
    AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> result =
        participantStoreClientsManager.getReader(CLUSTER_NAME);
    assertNotNull(result);
  }

}
