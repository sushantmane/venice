package com.linkedin.venice.throttle;

import com.google.common.util.concurrent.RateLimiter;


/**
 * A wrapper around Guava's RateLimiter to provide a common interface for rate limiting.
 */
public class GuavaRateLimiter implements VeniceRateLimiter {
  private final RateLimiter rateLimiter;
  private long permitsPerSecond;

  public GuavaRateLimiter(long permitsPerSecond) {
    this.permitsPerSecond = permitsPerSecond;
    this.rateLimiter = RateLimiter.create(permitsPerSecond);
  }

  @Override
  public boolean tryAcquirePermit(int units) {
    return rateLimiter.tryAcquire(units);
  }

  @Override
  public long getPermitsPerSecond() {
    return permitsPerSecond;
  }
}
