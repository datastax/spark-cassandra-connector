package com.datastax.spark.connector.cql

import scala.language.postfixOps

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import com.datastax.spark.connector.cql.CassandraConnectorConf.RetryDelayConf.{ExponentialDelay, LinearDelay, ConstantDelay}

class RetryDelayConfSpec extends FlatSpec with Matchers {

  "ConstantDelay" should "return the same delay regardless of the retry number" in {
    val d = ConstantDelay(1234 milliseconds)
    d.forRetry(1) shouldBe (1234 milliseconds)
    d.forRetry(2) shouldBe (1234 milliseconds)
    d.forRetry(3) shouldBe (1234 milliseconds)
  }

  "LinearDelay" should "return the calculated delay for different retry numbers" in {
    val d = LinearDelay(1234 milliseconds, 200 milliseconds)
    d.forRetry(1) shouldBe (1234 milliseconds)
    d.forRetry(2) shouldBe (1434 milliseconds)
    d.forRetry(3) shouldBe (1634 milliseconds)
  }

  "ExponentialDelay" should "return the calculated delay for different retry numbers" in {
    val d = ExponentialDelay(1200 milliseconds, 2.5d)
    d.forRetry(1) shouldBe (1200 milliseconds)
    d.forRetry(2) shouldBe (3000 milliseconds)
    d.forRetry(3) shouldBe (7500 milliseconds)
  }

}
