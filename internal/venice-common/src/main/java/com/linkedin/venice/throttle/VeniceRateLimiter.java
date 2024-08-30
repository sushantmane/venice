package com.linkedin.venice.throttle;

public interface VeniceRateLimiter {
  enum RateLimiterType {
    EVENT_THROTTLER, GUAVA_RATE_LIMITER, TOKEN_BUCKET_INCREMENTAL_REFILL, TOKEN_BUCKET_GREEDY_REFILL,
  }

  /**
   * Try to acquire permit for the given rcu. Will not block if permit is not available.
   * @param units Number of units to acquire.
   * @return true if permit is acquired, false otherwise.
   */
  boolean tryAcquirePermit(long units);

  /**
   * Acquire permit for the given rcu. Will block until permit is available.
   * @param units Number of units to acquire.
   * @return true if permit is acquired, false otherwise.
   * TODO: Implement this method for all the rate limiters.
   */
  default boolean acquirePermit(long units) {
    throw new UnsupportedOperationException("acquirePermit is not supported");
  }
}
