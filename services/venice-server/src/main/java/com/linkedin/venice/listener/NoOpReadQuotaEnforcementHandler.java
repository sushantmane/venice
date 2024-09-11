package com.linkedin.venice.listener;

import com.linkedin.venice.listener.request.RouterRequest;


/**
 * A no-op implementation of {@link QuotaEnforcementHandler} that allows all requests.
 */
public class NoOpReadQuotaEnforcementHandler implements QuotaEnforcementHandler {
  @Override
  public QuotaEnforcementResult enforceQuota(RouterRequest request) {
    return QuotaEnforcementResult.ALLOWED;
  }
}
