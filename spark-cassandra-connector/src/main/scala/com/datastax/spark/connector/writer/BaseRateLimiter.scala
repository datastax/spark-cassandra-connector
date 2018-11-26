package com.datastax.spark.connector.writer

/**
  * Represents a rate limiter.
  */
trait BaseRateLimiter {

  /**
    * Processes a single packet and it is up to the implementing class to determine whether
    * or not the thread should sleep.
    *
    * @param packetSize the size of the packet currently being processed
    */
  def maybeSleep(packetSize: Long): Unit

}
