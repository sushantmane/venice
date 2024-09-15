package com.linkedin.venice.fastclient;

import static com.linkedin.venice.fastclient.DispatchingAvroGenericStoreClient.URI_SEPARATOR;

import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.utils.EncodingUtils;


public class GetRequestContext extends RequestContext {
  private static final String QUERY_ACTION = QueryAction.STORAGE.toString().toLowerCase();

  private String resourceName = null;
  private int partitionId = -1;
  /**
   * This field is used to store the request uri to the backend.
   */
  private String requestUri = null;
  private RetryContext retryContext = null; // initialize if needed for retry
  private boolean isKeyEncodingRequired = false;
  private KeyEncodingType keyEncodingType = KeyEncodingType.NONE;
  private byte[] keyBytes = null;

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public RetryContext getRetryContext() {
    return retryContext;
  }

  public void setRetryContext(RetryContext retryContext) {
    this.retryContext = retryContext;
  }

  public boolean isKeyEncodingRequired() {
    return isKeyEncodingRequired;
  }

  public KeyEncodingType getKeyEncodingType() {
    return keyEncodingType;
  }

  private String getQueryActionString() {
    return QUERY_ACTION;
  }

  public void setKeyEncodingType(KeyEncodingType keyEncodingType) {
    this.isKeyEncodingRequired = keyEncodingType != KeyEncodingType.NONE;
    this.keyEncodingType = keyEncodingType;
  }

  public byte[] getKeyBytes() {
    return keyBytes;
  }

  public void setKeyBytes(byte[] keyBytes) {
    this.keyBytes = keyBytes;
  }

  public String getEncodedKey() {
    if (keyEncodingType == KeyEncodingType.BASE64) {
      return EncodingUtils.base64EncodeToString(keyBytes);
    }

    throw new IllegalArgumentException("Unsupported key encoding type: " + keyEncodingType);
  }

  private String getKeyEncodingSuffix() {
    if (keyEncodingType == KeyEncodingType.BASE64) {
      return "?f=b64";
    }
    return "";
  }

  static class RetryContext {
    boolean longTailRetryRequestTriggered;
    boolean errorRetryRequestTriggered;

    // TODO Explore whether adding a new boolean named originalWin to properly differentiate and
    // maybe add more strict tests around these 2 flags will be helpful.
    boolean retryWin;

    RetryContext() {
      longTailRetryRequestTriggered = false;
      errorRetryRequestTriggered = false;
      retryWin = false;
    }
  }

  public String getRequestUri() {
    if (requestUri != null) {
      return requestUri;
    }

    requestUri = URI_SEPARATOR + AbstractAvroStoreClient.TYPE_STORAGE + URI_SEPARATOR + resourceName + URI_SEPARATOR
        + partitionId + URI_SEPARATOR + getEncodedKey() + getKeyEncodingSuffix();

    return requestUri;
  }
}
