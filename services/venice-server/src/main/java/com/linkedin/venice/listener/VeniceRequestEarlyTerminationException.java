package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.netty.handler.codec.http.HttpResponseStatus;


public class VeniceRequestEarlyTerminationException extends VeniceException {
  private final String storeName;

  public VeniceRequestEarlyTerminationException(String storeName) {
    super(getMessage(storeName));
    this.storeName = storeName;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpResponseStatus.REQUEST_TIMEOUT.code();
  }

  public static VeniceReadResponseStatus getResponseStatusCode() {
    return VeniceReadResponseStatus.REQUEST_TIMEOUT;
  }

  public static String getMessage(String storeName) {
    return "The request to store: " + storeName + " is terminated because of early termination setup";
  }

  public String getStoreName() {
    return storeName;
  }
}
