package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.hadoop.VenicePushJob.INCREMENTAL_PUSH;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.LEGACY_AVRO_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.LEGACY_AVRO_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.MULTI_REGION;
import static com.linkedin.venice.hadoop.VenicePushJob.PARENT_CONTROLLER_REGION_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.PushJobSetting;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_ETL;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_KAFKA;
import static com.linkedin.venice.hadoop.VenicePushJob.StoreSetting;
import static com.linkedin.venice.hadoop.VenicePushJob.VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for VenicePushJob class.
 */
public class VenicePushJobTest {
  private static final String PUSH_JOB_ID = "push_job_number_101";
  private static final String DISCOVERY_URL = "d2://d2Clusters/venice-discovery";
  private static final String PARENT_REGION_NAME = "dc-parent";
  private static final String STORE_NAME = "test_feature_store";
  private static final String PARENT_ZK_ADDR = "zookeeper-li.com:2181";

  @Test(expectedExceptions = NullPointerException.class)
  public void testVenicePushJobThrowsNpeIfVpjPropertiesIsNull() {
    new VenicePushJob(PUSH_JOB_ID, null);
  }

  @Test
  public void testGetPushJobSettingThrowsUndefinedPropertyException() {
    Properties props = getVpjRequiredProperties();
    Set<Object> reqPropKeys = props.keySet();
    for (Object prop: reqPropKeys) {
      Properties propsCopy = (Properties) props.clone();
      propsCopy.remove(prop);
      try {
        new VenicePushJob(PUSH_JOB_ID, propsCopy);
        Assert.fail("Should throw UndefinedPropertyException for missing property: " + prop);
      } catch (UndefinedPropertyException expected) {
      }
    }
  }

  private Properties getVpjRequiredProperties() {
    Properties props = new Properties();
    props.put(VENICE_DISCOVER_URL_PROP, DISCOVERY_URL);
    props.put(MULTI_REGION, true);
    props.put(PARENT_CONTROLLER_REGION_NAME, PARENT_REGION_NAME);
    props.put(D2_ZK_HOSTS_PREFIX + PARENT_REGION_NAME, PARENT_ZK_ADDR);
    props.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    return props;
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Duplicate key field config found.*")
  public void testVenicePushJobCanHandleLegacyFieldsThrowsExceptionIfDuplicateKeysButValuesDiffer() {
    Properties props = getVpjRequiredProperties();
    props.put(LEGACY_AVRO_KEY_FIELD_PROP, "id");
    props.put(KEY_FIELD_PROP, "message");
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test
  public void testVenicePushJobCanHandleLegacyFields() {
    Properties props = getVpjRequiredProperties();
    props.put(LEGACY_AVRO_KEY_FIELD_PROP, "id");
    props.put(LEGACY_AVRO_VALUE_FIELD_PROP, "message");
    VenicePushJob vpj = new VenicePushJob(PUSH_JOB_ID, props);
    VeniceProperties veniceProperties = vpj.getVeniceProperties();
    assertNotNull(veniceProperties);
    assertEquals(veniceProperties.getString(KEY_FIELD_PROP), "id");
    assertEquals(veniceProperties.getString(VALUE_FIELD_PROP), "message");
  }

  @Test
  public void testGetPushJobSetting() {
    Properties props = getVpjRequiredProperties();
    VenicePushJob vpj = new VenicePushJob(PUSH_JOB_ID, props);
    PushJobSetting pushJobSetting = vpj.getPushJobSetting();
    assertNotNull(pushJobSetting);
    assertTrue(pushJobSetting.d2Routing);
  }

  @Test
  public void testGetPushJobSettingShouldNotUseD2RoutingIfControllerUrlDoesNotStartWithD2() {
    Properties props = getVpjRequiredProperties();
    props.put(VENICE_DISCOVER_URL_PROP, "http://venice.db:9898");
    VenicePushJob vpj = new VenicePushJob(PUSH_JOB_ID, props);
    PushJobSetting pushJobSetting = vpj.getPushJobSetting();
    assertNotNull(pushJobSetting);
    assertFalse(pushJobSetting.d2Routing);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Incremental push is not supported while using Kafka Input Format")
  public void testGetPushJobSettingShouldThrowExceptionIfSourceIsKafkaAndJobIsIncPush() {
    Properties props = getVpjRequiredProperties();
    props.put(SOURCE_KAFKA, true);
    props.put(INCREMENTAL_PUSH, true);
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Source ETL is not supported while using Kafka Input Format")
  public void testGetPushJobSettingShouldThrowExceptionWhenBothSourceKafkaAndEtlAreSet() {
    Properties props = getVpjRequiredProperties();
    props.put(SOURCE_KAFKA, true);
    props.put(SOURCE_ETL, true);
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test
  public void testShouldBuildDictionary() {
    PushJobSetting pushJobSetting = new PushJobSetting();
    StoreSetting storeSetting = new StoreSetting();

    pushJobSetting.compressionMetricCollectionEnabled = true;
    pushJobSetting.isIncrementalPush = false;
    storeSetting.compressionStrategy = CompressionStrategy.NO_OP;
    assertTrue(VenicePushJob.shouldBuildDictionary(pushJobSetting, storeSetting));

    pushJobSetting.compressionMetricCollectionEnabled = true;
    pushJobSetting.isIncrementalPush = true;
    storeSetting.compressionStrategy = CompressionStrategy.NO_OP;
    assertFalse(VenicePushJob.shouldBuildDictionary(pushJobSetting, storeSetting));

    pushJobSetting.compressionMetricCollectionEnabled = false;
    pushJobSetting.isIncrementalPush = false;
    storeSetting.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT;
    assertTrue(VenicePushJob.shouldBuildDictionary(pushJobSetting, storeSetting));

    pushJobSetting.compressionMetricCollectionEnabled = false;
    storeSetting.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT;
    pushJobSetting.isIncrementalPush = true;
    assertFalse(VenicePushJob.shouldBuildDictionary(pushJobSetting, storeSetting));

    pushJobSetting.compressionMetricCollectionEnabled = false;
    storeSetting.compressionStrategy = CompressionStrategy.NO_OP;
    assertFalse(VenicePushJob.shouldBuildDictionary(pushJobSetting, storeSetting));
  }
}
