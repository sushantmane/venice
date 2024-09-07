package com.linkedin.venice.response;

import io.netty.handler.codec.http.HttpResponseStatus;


/**
 * Defines response status codes for Venice read requests. This wrapper around {@link HttpResponseStatus} allows
 * for the inclusion of custom status codes that extend beyond the standard HTTP status codes.
 */
public enum VeniceReadResponseStatus {
  KEY_NOT_FOUND(HttpResponseStatus.NOT_FOUND), OK(HttpResponseStatus.OK), BAD_REQUEST(HttpResponseStatus.BAD_REQUEST),
  FORBIDDEN(HttpResponseStatus.FORBIDDEN), METHOD_NOT_ALLOWED(HttpResponseStatus.METHOD_NOT_ALLOWED),
  REQUEST_TIMEOUT(HttpResponseStatus.REQUEST_TIMEOUT), TOO_MANY_REQUESTS(HttpResponseStatus.TOO_MANY_REQUESTS),
  INTERNAL_SERVER_ERROR(HttpResponseStatus.INTERNAL_SERVER_ERROR),
  SERVICE_UNAVAILABLE(HttpResponseStatus.SERVICE_UNAVAILABLE),
  MISROUTED_STORE_VERSION(new HttpResponseStatus(570, "Misrouted request"));

  private final HttpResponseStatus httpResponseStatus;

  VeniceReadResponseStatus(HttpResponseStatus httpResponseStatus) {
    this.httpResponseStatus = httpResponseStatus;
  }

  public HttpResponseStatus getHttpResponseStatus() {
    return httpResponseStatus;
  }

  public int getCode() {
    return httpResponseStatus.code();
  }

  public static VeniceReadResponseStatus fromCode(int code) {
    for (VeniceReadResponseStatus status: values()) {
      if (status.getCode() == code) {
        return status;
      }
    }
    return null;
  }
}
