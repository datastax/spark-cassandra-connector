package com.datastax.spark.connector

import scala.concurrent.duration.FiniteDuration

package object embedded {

  /* Factor by which to scale timeouts during tests, e.g. to account for shared build system load. */
  implicit class SparkTestDuration(val duration: FiniteDuration) extends AnyVal {
    def dilated: FiniteDuration = (duration * 1.0).asInstanceOf[FiniteDuration]
  }
}
