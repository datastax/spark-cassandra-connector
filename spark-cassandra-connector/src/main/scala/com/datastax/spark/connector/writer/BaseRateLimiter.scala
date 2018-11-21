package com.datastax.spark.connector.writer

trait BaseRateLimiter {
  def maybeSleep(packetSize: Long): Unit
}
