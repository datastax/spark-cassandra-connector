package com.datastax.spark.connector.demo

import scala.concurrent.duration.{Duration, _}

/**
 * Simple helper functions.
 */
trait Assertions {

  /**
   * Await until the given condition evaluates to `true` or the timeout expires.
   */
  def awaitCond(condition: => Boolean, waitTime: Duration): Boolean = {
    val startTime = System.nanoTime.nanos.toMillis
    while (true) {
      if (!condition)
        return true
      if (System.currentTimeMillis() > startTime + waitTime.toMillis)
        return false
      Thread.sleep(waitTime.toMillis.min(100L))
    }
    // Should never go to here
    throw new RuntimeException("Conditional timed out.")
  }

}
