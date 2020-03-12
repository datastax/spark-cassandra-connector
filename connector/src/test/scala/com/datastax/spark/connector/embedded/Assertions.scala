package com.datastax.spark.connector.embedded

import scala.annotation.tailrec
import scala.concurrent.duration._

/**
 * Simple helper assertions. Some stolen from Akka akka.testkit.TestKit.scala for now.
 */
trait Assertions {

  /** Obtain current time (`System.nanoTime`) as Duration. */
  def now: FiniteDuration = System.nanoTime.nanos

  private var end: Duration = Duration.Undefined

  /**
   * Obtain time remaining for execution of the innermost enclosing `within`
   * block or missing that it returns the properly dilated default for this
   * case from settings (key "akka.test.single-expect-default").
   */
  def remainingOrDefault = remainingOr(1.seconds.dilated)

  /**
   * Obtain time remaining for execution of the innermost enclosing `within`
   * block or missing that it returns the given duration.
   */
  def remainingOr(duration: FiniteDuration): FiniteDuration = end match {
    case x if x eq Duration.Undefined => duration
    case x if !x.isFinite             => throw new IllegalArgumentException("`end` cannot be infinite")
    case f: FiniteDuration            => f - now
  }

  /**
   * Await until the given condition evaluates to `true` or the timeout
   * expires, whichever comes first.
   * If no timeout is given, take it from the innermost enclosing `within`
   * block.
   */
  def awaitCond(p: => Boolean, max: Duration = 3.seconds, interval: Duration = 100.millis, message: String = "") {
    val _max = remainingOrDilated(max)
    val stop = now + _max

    @tailrec
    def poll(t: Duration) {
      if (!p) {
        assert(now < stop, s"timeout ${_max} expired: $message")
        Thread.sleep(t.toMillis)
        poll((stop - now) min interval)
      }
    }

    poll(_max min interval)
  }

  private def remainingOrDilated(max: Duration): FiniteDuration = max match {
    case x if x eq Duration.Undefined => remainingOrDefault
    case x if !x.isFinite             => throw new IllegalArgumentException("max duration cannot be infinite")
    case f: FiniteDuration            => f.dilated
  }
}
