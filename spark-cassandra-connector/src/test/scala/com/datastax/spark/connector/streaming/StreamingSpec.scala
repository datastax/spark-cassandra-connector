package com.datastax.spark.connector.streaming

import scala.concurrent.duration._
import com.datastax.spark.connector.AbstractSpec

trait StreamingSpec extends AbstractSpec with SparkStreamingFixture {

  /* Keep in proportion with the above event num - not too long for CI without
* long-running sbt task exclusion.  */
  val events = 30

  val duration = 15.seconds

}
