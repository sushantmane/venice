package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Route;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_METADATA_VERSION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;
import static com.linkedin.venice.meta.Version.*;


/**
 * This class will add a new version to the given store.
 */
public class CreateVersion extends AbstractRoute {
  private static final Logger LOGGER = LogManager.getLogger(CreateVersion.class);
  private final boolean checkReadMethodForKafka;
  private final boolean disableParentRequestTopicForStreamPushes;

  public CreateVersion(Optional<DynamicAccessController> accessController, boolean checkReadMethodForKafka,
      boolean disableParentRequestTopicForStreamPushes) {
    super(accessController);
    this.checkReadMethodForKafka = checkReadMethodForKafka;
    this.disableParentRequestTopicForStreamPushes = disableParentRequestTopicForStreamPushes;
  }

  /**
   * Instead of asking Venice to create a version, pushes should ask venice which topic to write into.
   * The logic below includes the ability to respond with an existing topic for the same push, allowing requests
   * to be idempotent
   *
   * @param admin
   * @return
   */
  public Route requestTopicForPushing(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow allowList users to run this command
        if (!isAllowListUser(request)
            && (!hasWriteAccessToTopic(request) || (this.checkReadMethodForKafka && !hasReadAccessToTopic(request))))
        {
          response.status(HttpStatus.SC_FORBIDDEN);
          String userId = getPrincipalId(request);
          String storeName = request.queryParams(NAME);

          /**
           * When partners have ACL issues for their push, we should provide an accurate and informative messages that
           * help partners to unblock by themselves.
           */
          String errorMsg;
          boolean missingWriteAccess = !hasWriteAccessToTopic(request);
          boolean missingReadAccess = this.checkReadMethodForKafka && !hasReadAccessToTopic(request);
          if (missingWriteAccess && missingReadAccess) {
            errorMsg = "[Error] Push terminated due to ACL issues for user \"" + userId
                + "\". Please visit go/veniceacl and setup [write] ACLs for your store.";
          } else if (missingWriteAccess) {
            errorMsg = "[Error] Hadoop user \"" + userId + "\" does not have [write] permission for store: "
                + storeName + ". Please refer to go/veniceacl and setup store ACLs";
          } else {
            errorMsg = "[Error] Missing [read] method in [write] ACLs for user \"" + userId
                + "\". Please visit go/veniceacl and setup ACLs for your store";
          }
          responseObject.setError(errorMsg);
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }

        AdminSparkServer.validateParams(request, REQUEST_TOPIC.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        Store store = admin.getStore(clusterName, storeName);
        if (null == store) {
          throw new VeniceNoStoreException(storeName);
        }
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setDaVinciPushStatusStoreEnabled(store.isDaVinciPushStatusStoreEnabled());

        // Retrieve partitioner config from the store
        PartitionerConfig storePartitionerConfig = store.getPartitionerConfig();
        if (null == request.queryParams(PARTITIONERS)) {
          // Request does not contain partitioner info
          responseObject.setPartitionerClass(storePartitionerConfig.getPartitionerClass());
          responseObject.setAmplificationFactor(storePartitionerConfig.getAmplificationFactor());
          responseObject.setPartitionerParams(storePartitionerConfig.getPartitionerParams());
        } else {
          // Retrieve provided partitioner class list from the request
          boolean hasMatchedPartitioner = false;
          for (String partitioner : request.queryParams(PARTITIONERS).split(",")) {
            if (partitioner.equals(storePartitionerConfig.getPartitionerClass())) {
              responseObject.setPartitionerClass(storePartitionerConfig.getPartitionerClass());
              responseObject.setAmplificationFactor(storePartitionerConfig.getAmplificationFactor());
              responseObject.setPartitionerParams(storePartitionerConfig.getPartitionerParams());
              hasMatchedPartitioner = true;
              break;
            }
          }
          if (!hasMatchedPartitioner) {
            throw new VeniceException("Expected partitioner class " + storePartitionerConfig.getPartitionerClass() + " cannot be found.");
          }
        }

        if (!store.isLeaderFollowerModelEnabled()
            && store.getPartitionerConfig() != null && store.getPartitionerConfig().getAmplificationFactor() != 1) {
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "amplificationFactor can only be specified "
              + "when leaderFollower enabled", ErrorType.BAD_REQUEST);
        }

        String pushTypeString = request.queryParams(PUSH_TYPE);
        PushType pushType;
        try {
          pushType = PushType.valueOf(pushTypeString);
        } catch (RuntimeException e){
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, pushTypeString + " is an invalid " + PUSH_TYPE, e, ErrorType.BAD_REQUEST);
        }
        validatePushType(pushType, store);

        boolean sendStartOfPush = false;
        // Make this optional so that it is compatible with old version controller client
        if (request.queryParams().contains(SEND_START_OF_PUSH)) {
          sendStartOfPush = Utils.parseBooleanFromString(request.queryParams(SEND_START_OF_PUSH), SEND_START_OF_PUSH);
        }

        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
        int partitionCount = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
        responseObject.setReplicas(replicationFactor);
        responseObject.setPartitions(partitionCount);

        boolean isSSL = admin.isSSLEnabledForPush(clusterName, storeName);
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers(isSSL));
        responseObject.setEnableSSL(isSSL);

        String pushJobId = request.queryParams(PUSH_JOB_ID);

        boolean sorted = false; // an inefficient but safe default
        String sortedParam = request.queryParams(PUSH_IN_SORTED_ORDER);
        if (sortedParam != null) {
          sorted = Utils.parseBooleanFromString(sortedParam, PUSH_IN_SORTED_ORDER);
        }

        boolean isWriteComputeEnabled = false;
        String wcEnabledParam = request.queryParams(IS_WRITE_COMPUTE_ENABLED);
        if (wcEnabledParam != null) {
          isWriteComputeEnabled = Utils.parseBooleanFromString(wcEnabledParam, IS_WRITE_COMPUTE_ENABLED);
        }

        Optional<String> sourceGridFabric = Optional.ofNullable(request.queryParams(SOURCE_GRID_FABRIC));

        /**
         * We can't honor source grid fabric and emergency source region config untill the store is A/A enabled in all regions. This is because
         * if push job start producing to a different prod region then non A/A enabled region will not have the capability to consume from that region.
         * This resets this config in such cases.
         */
        Lazy<Boolean> isActiveActiveReplicationEnabledInAllRegion = Lazy.of(() -> {
          if (admin.isParent() && store.isActiveActiveReplicationEnabled()) {
            return admin.isActiveActiveReplicationEnabledInAllRegion(clusterName, storeName, false);
          } else {
            return false;
          }
        });

        Lazy<Boolean> isActiveActiveReplicationEnabledInAllRegionAllVersions = Lazy.of(() -> {
          if (admin.isParent() && store.isActiveActiveReplicationEnabled()) {
            return admin.isActiveActiveReplicationEnabledInAllRegion(clusterName, storeName, true);
          } else {
            return false;
          }
        });

        if (sourceGridFabric.isPresent() && !isActiveActiveReplicationEnabledInAllRegion.get()) {
          LOGGER.info("Ignoring config " + SOURCE_GRID_FABRIC + " : " + sourceGridFabric.get() + ", as store " + storeName + " is not set up for Active/Active replication in all regions");
          sourceGridFabric = Optional.empty();
        }
        Optional<String> emergencySourceRegion = admin.getEmergencySourceRegion();
        if (emergencySourceRegion.isPresent() && !isActiveActiveReplicationEnabledInAllRegion.get()) {
          LOGGER.info("Ignoring config " + EMERGENCY_SOURCE_REGION + " : " + emergencySourceRegion.get() + ", as store " + storeName + " is not set up for Active/Active replication in all regions");
          emergencySourceRegion = Optional.empty();
        }
        LOGGER.info("requestTopicForPushing: source grid fabric: " + sourceGridFabric.orElse("") + ", emergency source region: " + emergencySourceRegion.orElse(""));

        /**
         * Version-level rewind time override, and it is only valid for hybrid stores.
         */
        Optional<String> rewindTimeInSecondsOverrideOptional = Optional.ofNullable(request.queryParams(REWIND_TIME_IN_SECONDS_OVERRIDE));
        long rewindTimeInSecondsOverride = -1;
        if (rewindTimeInSecondsOverrideOptional.isPresent()) {
          rewindTimeInSecondsOverride = Long.parseLong(rewindTimeInSecondsOverrideOptional.get());
        }

        /**
         * Version level override to defer marking this new version to the serving version post push completion.
         */
        boolean deferVersionSwap = Boolean.parseBoolean(request.queryParams(DEFER_VERSION_SWAP));

        switch(pushType) {
          case BATCH:
          case INCREMENTAL:
          case STREAM_REPROCESSING:
            if (!admin.whetherEnableBatchPushFromAdmin(storeName)) {
              throw new VeniceUnsupportedOperationException(pushTypeString, "Please push data to Venice Parent Colo instead");
            }
            String dictionaryStr = request.queryParams(COMPRESSION_DICTIONARY);

            /**
             * Before trying to get the version, create the RT topic in parent kafka since it's needed anyway in following cases.
             * Otherwise topic existence check fails internally.
             */
            if (pushType.isIncremental()) {
              admin.getRealTimeTopic(clusterName, storeName);
              if (store.isApplyTargetVersionFilterForIncPush()) {
                int targetVersion;
                if (admin.isParent()) {
                  Map<String, Integer> regionToCurrentVersions = admin.getCurrentVersionsForMultiColos(clusterName, storeName);
                  if (regionToCurrentVersions == null || regionToCurrentVersions.isEmpty()) {
                    throw new VeniceException(
                        "Failed to get current versions from different regions in parent controller " + "for store " + storeName + " during incremental push");
                  }
                  targetVersion = regionToCurrentVersions.entrySet().iterator().next().getValue();
                  for (Map.Entry<String, Integer> regionToCurrentVersion : regionToCurrentVersions.entrySet()) {
                    if (regionToCurrentVersion.getValue() != targetVersion) {
                      throw new VeniceException(
                          "Current version for store " + storeName + " is " + regionToCurrentVersion.getValue() + " in region " + regionToCurrentVersion.getKey()
                              + ", which is different from other regions. " + "Failing the incremental push until there is a consistent current version across all regions");
                    }
                  }
                } else {
                  targetVersion = store.getCurrentVersion();
                }
                /**
                 * Set the store's current version into the response object.
                 */
                responseObject.setTargetVersionForIncPush(targetVersion);
              }
            }

            final Optional<X509Certificate> certInRequest = isAclEnabled() ? Optional.of(getCertificate(request)) : Optional.empty();
            final Version version =
                admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionCount, replicationFactor,
                   pushType, sendStartOfPush, sorted, dictionaryStr, sourceGridFabric, certInRequest, rewindTimeInSecondsOverride, emergencySourceRegion, deferVersionSwap);

            // If Version partition count different from calculated partition count use the version count as store count
            // may have been updated later.
            if (version.getPartitionCount() != partitionCount) {
              responseObject.setPartitions(version.getPartitionCount());
            }
            String responseTopic;
            boolean overrideSourceFabric = true;
            if (pushType.isStreamReprocessing()) {
              responseTopic = Version.composeStreamReprocessingTopic(storeName, version.getNumber());
            } else if (pushType.isIncremental()) {
              responseTopic = Version.composeRealTimeTopic(storeName);
              // disable amplificationFactor logic on real-time topic
              responseObject.setAmplificationFactor(1);

              if (version.isNativeReplicationEnabled()) {
                /**
                 * For incremental push with RT policy store the push job produces to parent corp kafka cluster. We should not override the
                 * source fabric in such cases with NR source fabric.
                 */
                overrideSourceFabric = false;
              }
            } else {
              responseTopic = version.kafkaTopicName();
            }

            responseObject.setVersion(version.getNumber());
            responseObject.setKafkaTopic(responseTopic);
            responseObject.setCompressionStrategy(version.getCompressionStrategy());
            if (version.isNativeReplicationEnabled() && overrideSourceFabric) {
              String childDataCenterKafkaBootstrapServer = version.getPushStreamSourceAddress();
              if (childDataCenterKafkaBootstrapServer != null) {
                responseObject.setKafkaBootstrapServers(childDataCenterKafkaBootstrapServer);
              }
            }

            /**
             * Override the source fabric for H2V incremental push job for A/A replication. Following is the order of priority.
             * 1. Parent controller emergency source fabric config.
             * 2. H2V plugin source grid fabric config.
             * 3. parent corp kafka cluster.
             *
             * At this point parent corp cluster is already set in the responseObject.setKafkaBootstrapServers().
             * So we only need to override for 1 and 2.
             */
            if (pushType.isIncremental() && admin.isParent()) {
              if (isActiveActiveReplicationEnabledInAllRegion.get()) {
                Optional<String> overRideSourceRegion =
                    emergencySourceRegion.isPresent() ? emergencySourceRegion : Optional.empty();
                if (!overRideSourceRegion.isPresent() && sourceGridFabric.isPresent()) {
                  overRideSourceRegion = sourceGridFabric;
                }
                if (overRideSourceRegion.isPresent()) {
                  Pair<String, String> sourceKafkaBootstrapServersAndZk = admin.getNativeReplicationKafkaBootstrapServerAndZkAddress(overRideSourceRegion.get());
                  String sourceKafkaBootstrapServers = sourceKafkaBootstrapServersAndZk.getFirst();
                  if (sourceKafkaBootstrapServers == null) {
                    throw new VeniceException("Failed to get the kafka URL for the source region: " + overRideSourceRegion.get());
                  }
                  responseObject.setKafkaBootstrapServers(sourceKafkaBootstrapServers);
                  LOGGER.info("Incremental Push to RT Policy job source region is being overridden with: " + overRideSourceRegion.get() + " address:" + sourceKafkaBootstrapServers);
                }
              } else if (version.isNativeReplicationEnabled()) {
                Optional<String> aggregateRealTimeTopicSourceKafkaServers = admin.getAggregateRealTimeTopicSource(clusterName);
                aggregateRealTimeTopicSourceKafkaServers.ifPresent(source -> responseObject.setKafkaBootstrapServers(source));
              }
              LOGGER.info("Incremental Push to RT Policy job final source region address is: " + responseObject.getKafkaBootstrapServers());
            }
            break;
          case STREAM:
            if (admin.isParent()) {
              // Conditionally check if the controller allows for fetching this information
              if (disableParentRequestTopicForStreamPushes) {
                throw new VeniceException(String.format("Parent request topic is disabled!!  Cannot push data to topic in parent colo for store %s.  Aborting!!", storeName));
              }
              // Conditionally check if this store has aggregate mode enabled.  If not, throw an exception (as aggregate mode is required to produce to parent colo)
              // We check the store config instead of the version config because we want this policy to go into effect without needing to perform empty pushes everywhere
              if (!store.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE)) {
                if (!isActiveActiveReplicationEnabledInAllRegionAllVersions.get()) {
                  throw new VeniceException("Store is not in aggregate mode!  Cannot push data to parent topic!!");
                } else {
                  LOGGER.info("Store: " + storeName + " samza job running in Aggregate mode, Store config is in Non-Aggregate mode, AA is enabled in all regions, letting the job continue");
                }
              }
            } else {
              if (store.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE)) {
                if (!store.isActiveActiveReplicationEnabled()) {
                  throw new VeniceException("Store is in aggregate mode!  Cannot push data to child topic!!");
                } else {
                  LOGGER.info("Store: " + storeName + " samza job running in Non-Aggregate mode, Store config is in Aggregate mode, AA is enabled in the local region, letting the job continue");
                }
              }
            }

            String realTimeTopic = admin.getRealTimeTopic(clusterName, storeName);
            responseObject.setKafkaTopic(realTimeTopic);
            // disable amplificationFactor logic on real-time topic
            responseObject.setAmplificationFactor(1);
            break;
          default:
            throw new VeniceException(pushTypeString + " is an unrecognized " + PUSH_TYPE);
        }
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }

      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  private void validatePushType(PushType pushType, Store store) {
    if (pushType.equals(PushType.STREAM) && !store.isHybrid()){
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "requesting topic for streaming writes to store "
          + store.getName() + " which is not configured to be a hybrid store", ErrorType.BAD_REQUEST);
    }
    if (pushType.equals(PushType.STREAM) && store.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.NONE)) {
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "requesting topic for streaming writes to store " +
          store.getName() + " which is configured to have a hybrid data replication policy " +
          store.getHybridStoreConfig().getDataReplicationPolicy(), ErrorType.BAD_REQUEST);
    }
    DataReplicationPolicy drPolicy = store.isHybrid() ? store.getHybridStoreConfig().getDataReplicationPolicy() : null;
    if (pushType.isIncremental() && drPolicy != DataReplicationPolicy.AGGREGATE && drPolicy != DataReplicationPolicy.ACTIVE_ACTIVE) {
      LOGGER.error("Requested push type is not compatible with current store configs. To use incremental push store needs"
              + " to be of hybrid type with either {} or {} replication policy. Store:{} isHybrid:{} dataReplicationPolicy:{}",
          DataReplicationPolicy.ACTIVE_ACTIVE, DataReplicationPolicy.AGGREGATE, store.getName(), store.isHybrid(), drPolicy);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Push type is not compatible with store type. "
          + "To use incremental push store needs to be of hybrid type with either AGGREGATE or ACTIVE_ACTIVE replication policy.",
          ErrorType.BAD_REQUEST);
    }
  }

  /**
   * This function is only being used by store migration parent controllers, which write add version admin message.
   */
  public Route addVersionAndStartIngestion(Admin admin) {
    return (request, response) -> {
      VersionResponse responseObject = new VersionResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow allowlist users to run this command
        if (!isAllowListUser(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("ACL failed for request " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, ADD_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String pushJobId = request.queryParams(PUSH_JOB_ID);
        int versionNumber = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        int partitionCount = Utils.parseIntFromString(request.queryParams(PARTITION_COUNT), PARTITION_COUNT);
        PushType pushType;
        try {
          pushType = PushType.valueOf(request.queryParams(PUSH_TYPE));
        } catch (RuntimeException parseException) {
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, request.queryParams(PUSH_TYPE) + " is an invalid "
              + PUSH_TYPE, parseException, ErrorType.BAD_REQUEST);
        }
        String remoteKafkaBootstrapServers = null;
        if (request.queryParams().contains(REMOTE_KAFKA_BOOTSTRAP_SERVERS)) {
          remoteKafkaBootstrapServers = request.queryParams(REMOTE_KAFKA_BOOTSTRAP_SERVERS);
        }

        /**
         * Version-level rewind time override, and it is only valid for hybrid stores.
         */
        Optional<String> rewindTimeInSecondsOverrideOptional = Optional.ofNullable(request.queryParams(REWIND_TIME_IN_SECONDS_OVERRIDE));
        long rewindTimeInSecondsOverride = -1;
        if (rewindTimeInSecondsOverrideOptional.isPresent()) {
          rewindTimeInSecondsOverride = Long.parseLong(rewindTimeInSecondsOverrideOptional.get());
        }

        Optional<String> replicationMetadataVersionIdOptional = Optional.ofNullable(request.queryParams(REPLICATION_METADATA_VERSION_ID));
        int replicationMetadataVersionId = REPLICATION_METADATA_VERSION_ID_UNSET;
        if (replicationMetadataVersionIdOptional.isPresent()) {
          replicationMetadataVersionId = Integer.parseInt(replicationMetadataVersionIdOptional.get());
        }

        admin.addVersionAndStartIngestion(clusterName, storeName, pushJobId, versionNumber, partitionCount, pushType,
            remoteKafkaBootstrapServers, rewindTimeInSecondsOverride, replicationMetadataVersionId, false);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setVersion(versionNumber);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  @Deprecated
  public Route uploadPushInfo(Admin admin){
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow allowlist users to run this command
        if (!isAllowListUser(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("ACL failed for request " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, OFFLINE_PUSH_INFO.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        // TODO No-op, can be removed once the corresponding H2V plugin version is deployed.
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);

    };
  }

  public Route writeEndOfPush(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow allowlist users to run this command
        if (!isAllowListUser(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("You don't have permission to end this push job; please grant write ACL for yourself.");
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, END_OF_PUSH.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String versionString = request.queryParams(VERSION);
        int versionNumber = Integer.parseInt(versionString);

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        admin.writeEndOfPush(clusterName, storeName, versionNumber, false);

      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route emptyPush(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      response.type(HttpConstants.JSON);
      String clusterName = null;
      Version version = null;
      try {
        // Only allow allowlist users to run this command
        if (!isAllowListUser(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, EMPTY_PUSH.getParams(), admin);

        String storeName = request.queryParams(NAME);
        if (!admin.whetherEnableBatchPushFromAdmin(storeName)) {
          throw new VeniceUnsupportedOperationException("EMPTY PUSH",
              "Please push data to Venice Parent Colo instead or use Aggregate mode if you are running Samza GF Job.");
        }

        clusterName = request.queryParams(CLUSTER);
        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        String pushJobId = request.queryParams(PUSH_JOB_ID);
        int partitionNum = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
        version = admin.incrementVersionIdempotent(clusterName, storeName, pushJobId,
            partitionNum, replicationFactor);
        int versionNumber = version.getNumber();

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setVersion(versionNumber);
        responseObject.setPartitions(partitionNum);
        responseObject.setReplicas(replicationFactor);
        responseObject.setKafkaTopic(version.kafkaTopicName());

        admin.writeEndOfPush(clusterName, storeName, versionNumber, true);

        /** TODO: Poll {@link com.linkedin.venice.controller.VeniceParentHelixAdmin#getOffLineJobStatus(String, String, Map, TopicManager)} until it is terminal... */

      } catch (Throwable e) {
        // Clean up on failed push.
        if (version != null && clusterName != null) {
          LOGGER.warn("Cleaning up failed Empty push of " + version.kafkaTopicName());
          admin.killOfflinePush(clusterName, version.kafkaTopicName(), true);
        }
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
