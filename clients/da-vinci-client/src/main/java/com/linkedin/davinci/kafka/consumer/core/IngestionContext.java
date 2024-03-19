package com.linkedin.davinci.kafka.consumer.core;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.davinci.kafka.consumer.AbstractStoreBufferService;
import com.linkedin.davinci.kafka.consumer.AggKafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.ParticipantStoreConsumptionTask;
import com.linkedin.davinci.kafka.consumer.RemoteIngestionRepairService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTaskFactory;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.view.VeniceViewWriterFactory;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.locks.ResourceAutoClosableLockManager;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Queue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class IngestionContext {
  private static final Logger LOGGER = LogManager.getLogger(IngestionContext.class);

  public IngestionContext(IngestionContextBuilder builder) {
  }

  public static class IngestionContextBuilder {
    private VeniceIngestionBackend ingestionBackend;
    private VeniceConfigLoader configService;
    private ReadOnlyStoreRepository readOnlyStoreRepository;
    private VeniceWriterFactory veniceWriterFactory;
    private HeartbeatMonitoringService heartbeatMonitoringService;
    private VeniceViewWriterFactory veniceViewWriterFactory;
    private StorageEngineRepository storageEngineRepository;
    private StorageMetadataService storageMetadataService;
    private Queue<VeniceNotifier> leaderFollowerNotifiers;
    private ReadOnlySchemaRepository schemaRepo;
    private TopicManagerRepository topicManagerRepository;
    private AggHostLevelIngestionStats aggHostLevelIngestionStats;
    private AggVersionedDIVStats aggVersionedDIVStats;
    private AggVersionedIngestionStats aggVersionedIngestionStats;
    private AbstractStoreBufferService storeBufferService;
    private VeniceServerConfig veniceServerConfig;
    private DiskUsage diskUsage;
    private AggKafkaConsumerService aggKafkaConsumerService;
    private InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;
    private RemoteIngestionRepairService remoteIngestionRepairService;
    private MetaStoreWriter metaStoreWriter;
    private StorageEngineBackedCompressorFactory compressorFactory;
    private PubSubTopicRepository pubSubTopicRepository;
    private Runnable runnableForKillIngestionTasksForNonCurrentVersions;
    private boolean isDaVinciClient;
    private boolean isIsolatedIngestion;
    private SchemaReader pubSubMessageValueSchemaReader;
    private StoreIngestionTaskFactory storeIngestionTaskFactory;
    private ParticipantStoreConsumptionTask participantStoreConsumptionTask;
    private ObjectCacheBackend objectCacheBackend;
    private DaVinciRecordTransformer daVinciRecordTransformer;
    private PubSubProducerAdapterFactory pubSubProducerAdapterFactory;
    private StorageEngineBackedCompressorFactory storageEngineBackedCompressorFactory;
    private ResourceAutoClosableLockManager<String> topicLockManager;
    private KafkaValueSerializer kafkaValueSerializer;

    public IngestionContextBuilder() {
    }

    public IngestionContextBuilder setIngestionBackend(VeniceIngestionBackend ingestionBackend) {
      this.ingestionBackend = ingestionBackend;
      return this;
    }

    public IngestionContextBuilder setConfigService(VeniceConfigLoader configService) {
      this.configService = configService;
      return this;
    }

    public IngestionContextBuilder setReadOnlyStoreRepository(ReadOnlyStoreRepository readOnlyStoreRepository) {
      this.readOnlyStoreRepository = readOnlyStoreRepository;
      return this;
    }

    public IngestionContextBuilder setVeniceWriterFactory(VeniceWriterFactory veniceWriterFactory) {
      this.veniceWriterFactory = veniceWriterFactory;
      return this;
    }

    public IngestionContextBuilder setHeartbeatMonitoringService(
        HeartbeatMonitoringService heartbeatMonitoringService) {
      this.heartbeatMonitoringService = heartbeatMonitoringService;
      return this;
    }

    public IngestionContextBuilder setVeniceViewWriterFactory(VeniceViewWriterFactory veniceViewWriterFactory) {
      this.veniceViewWriterFactory = veniceViewWriterFactory;
      return this;
    }

    public IngestionContextBuilder setStorageEngineRepository(StorageEngineRepository storageEngineRepository) {
      this.storageEngineRepository = storageEngineRepository;
      return this;
    }

    public IngestionContextBuilder setStorageMetadataService(StorageMetadataService storageMetadataService) {
      this.storageMetadataService = storageMetadataService;
      return this;
    }

    public IngestionContextBuilder setLeaderFollowerNotifiers(Queue<VeniceNotifier> leaderFollowerNotifiers) {
      this.leaderFollowerNotifiers = leaderFollowerNotifiers;
      return this;
    }

    public IngestionContextBuilder setSchemaRepo(ReadOnlySchemaRepository schemaRepo) {
      this.schemaRepo = schemaRepo;
      return this;
    }

    public IngestionContextBuilder setTopicManagerRepository(TopicManagerRepository topicManagerRepository) {
      this.topicManagerRepository = topicManagerRepository;
      return this;
    }

    public IngestionContextBuilder setAggHostLevelIngestionStats(
        AggHostLevelIngestionStats aggHostLevelIngestionStats) {
      this.aggHostLevelIngestionStats = aggHostLevelIngestionStats;
      return this;
    }

    public IngestionContextBuilder setAggVersionedDIVStats(AggVersionedDIVStats aggVersionedDIVStats) {
      this.aggVersionedDIVStats = aggVersionedDIVStats;
      return this;
    }

    public IngestionContextBuilder setAggVersionedIngestionStats(
        AggVersionedIngestionStats aggVersionedIngestionStats) {
      this.aggVersionedIngestionStats = aggVersionedIngestionStats;
      return this;
    }

    public IngestionContextBuilder setStoreBufferService(AbstractStoreBufferService storeBufferService) {
      this.storeBufferService = storeBufferService;
      return this;
    }

    public IngestionContextBuilder setVeniceServerConfig(VeniceServerConfig veniceServerConfig) {
      this.veniceServerConfig = veniceServerConfig;
      return this;
    }

    public IngestionContextBuilder setDiskUsage(DiskUsage diskUsage) {
      this.diskUsage = diskUsage;
      return this;
    }

    public IngestionContextBuilder setAggKafkaConsumerService(AggKafkaConsumerService aggKafkaConsumerService) {
      this.aggKafkaConsumerService = aggKafkaConsumerService;
      return this;
    }

    public IngestionContextBuilder setPartitionStateSerializer(
        InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
      this.partitionStateSerializer = partitionStateSerializer;
      return this;
    }

    public IngestionContextBuilder setRemoteIngestionRepairService(
        RemoteIngestionRepairService remoteIngestionRepairService) {
      this.remoteIngestionRepairService = remoteIngestionRepairService;
      return this;
    }

    public IngestionContextBuilder setMetaStoreWriter(MetaStoreWriter metaStoreWriter) {
      this.metaStoreWriter = metaStoreWriter;
      return this;
    }

    public IngestionContextBuilder setCompressorFactory(StorageEngineBackedCompressorFactory compressorFactory) {
      this.compressorFactory = compressorFactory;
      return this;
    }

    public IngestionContextBuilder setPubSubTopicRepository(PubSubTopicRepository pubSubTopicRepository) {
      this.pubSubTopicRepository = pubSubTopicRepository;
      return this;
    }

    public IngestionContextBuilder setRunnableForKillIngestionTasksForNonCurrentVersions(
        Runnable runnableForKillIngestionTasksForNonCurrentVersions) {
      this.runnableForKillIngestionTasksForNonCurrentVersions = runnableForKillIngestionTasksForNonCurrentVersions;
      return this;
    }

    public IngestionContextBuilder setIsDaVinciClient(boolean isDaVinciClient) {
      this.isDaVinciClient = isDaVinciClient;
      return this;
    }

    public IngestionContextBuilder setIsIsolatedIngestion(boolean isIsolatedIngestion) {
      this.isIsolatedIngestion = isIsolatedIngestion;
      return this;
    }

    public IngestionContextBuilder setPubSubMessageValueSchemaReader(SchemaReader pubSubMessageValueSchemaReader) {
      this.pubSubMessageValueSchemaReader = pubSubMessageValueSchemaReader;
      return this;
    }

    public IngestionContextBuilder setStoreIngestionTaskFactory(StoreIngestionTaskFactory storeIngestionTaskFactory) {
      this.storeIngestionTaskFactory = storeIngestionTaskFactory;
      return this;
    }

    public IngestionContextBuilder setParticipantStoreConsumptionTask(
        ParticipantStoreConsumptionTask participantStoreConsumptionTask) {
      this.participantStoreConsumptionTask = participantStoreConsumptionTask;
      return this;
    }

    public IngestionContextBuilder setObjectCacheBackend(ObjectCacheBackend objectCacheBackend) {
      this.objectCacheBackend = objectCacheBackend;
      return this;
    }

    public IngestionContextBuilder setDaVinciRecordTransformer(DaVinciRecordTransformer daVinciRecordTransformer) {
      this.daVinciRecordTransformer = daVinciRecordTransformer;
      return this;
    }

    public IngestionContextBuilder setPubSubProducerAdapterFactory(
        PubSubProducerAdapterFactory pubSubProducerAdapterFactory) {
      this.pubSubProducerAdapterFactory = pubSubProducerAdapterFactory;
      return this;
    }

    public IngestionContextBuilder setStorageEngineBackedCompressorFactory(
        StorageEngineBackedCompressorFactory storageEngineBackedCompressorFactory) {
      this.storageEngineBackedCompressorFactory = storageEngineBackedCompressorFactory;
      return this;
    }

    public IngestionContextBuilder setTopicLockManager(ResourceAutoClosableLockManager<String> topicLockManager) {
      this.topicLockManager = topicLockManager;
      return this;
    }

    public IngestionContextBuilder setKafkaValueSerializer(KafkaValueSerializer kafkaValueSerializer) {
      this.kafkaValueSerializer = kafkaValueSerializer;
      return this;
    }

    public IngestionContext build() {
      return new IngestionContext(this);
    }
  }
}
