package com.datastax.spark.connector

import java.net.InetAddress

import scala.concurrent.duration.FiniteDuration

package object embedded {

  implicit val ZookeeperConnectionString = s"${InetAddress.getLocalHost.getHostAddress}:2181"

  /* Factor by which to scale timeouts during tests, e.g. to account for shared build system load. */
  implicit class SparkTestDuration(val duration: FiniteDuration) extends AnyVal {
    def dilated: FiniteDuration = (duration * 1.0).asInstanceOf[FiniteDuration]
  }
}
