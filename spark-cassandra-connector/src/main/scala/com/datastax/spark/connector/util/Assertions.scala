package com.datastax.spark.connector.util

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.duration.{FiniteDuration, Duration}

/**
 * Simple helper functions.
 */
trait Assertions {

  /**
   * Await until the given condition evaluates to `true` or the timeout expires.
   */
  def awaitCond(p: => Boolean, max: Duration, interval: Duration = 100.millis, noThrow: Boolean = false): Boolean = {
    val stop = now + max

    @tailrec
    def poll(): Boolean = {
      if (!p) {
        val toSleep = stop - now
        if (toSleep <= Duration.Zero) {
          if (noThrow) false
          else throw new AssertionError(s"timeout $max expired")
        } else {
          Thread.sleep((toSleep min interval).toMillis)
          poll()
        }
      } else true
    }

    poll()
  }


  /**
   * Obtain current time (`System.nanoTime`) as Duration.
   */
  def now: FiniteDuration = System.nanoTime.nanos

}
