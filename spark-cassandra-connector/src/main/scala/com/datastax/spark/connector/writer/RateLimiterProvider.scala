package com.datastax.spark.connector.writer

/**
  * Represents a provider that creates and returns a rate limiter with possible configuration.
  */
trait RateLimiterProvider {
  /**
    * Given a set of arguments, instantiates and returns a rate limiter.
    *
    * @param args sequence of arguments that can customize the returned rate limiter
    * @return the created rate limiter
    */
  def getRateLimiterWithConf(args: Any*): BaseRateLimiter
}