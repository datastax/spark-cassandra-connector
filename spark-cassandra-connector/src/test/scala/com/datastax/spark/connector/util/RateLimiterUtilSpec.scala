package com.datastax.spark.connector.util

import java.lang.Thread.sleep

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import com.datastax.spark.connector.writer.{BaseRateLimiter, RateLimiterProvider}
import main.scala.com.datastax.spark.connector.writer.LeakyBucketRateLimiterProvider

class RateLimiterUtilSpec extends FlatSpec with Matchers {

  "RateLimiterUtil" should "return a custom rate limiter provider should that be specified" in {
    val mockProvider = new MockProvider()
    val rateLimiter = RateLimiterUtil.getRateLimiter(mockProvider.getClass.getName)
    rateLimiter.getClass.getName should equal (mockProvider.getRateLimiterWithConf().getClass.getName)
  }

  it should "throw an error when custom rate limiter provider cannot be instantiated" in {
    a [ClassNotFoundException] should be thrownBy RateLimiterUtil.getRateLimiter("non.existing.class")
    an [InstantiationException] should be thrownBy RateLimiterUtil.getRateLimiter(NonInstantiable.getClass.getName)
  }

  // mock object that cannot be instantiated
  object NonInstantiable {}
}

// mock provider with public constructor that can be instantiated
class MockProvider extends RateLimiterProvider {
  {}

  override def getRateLimiterWithConf(args: Any*): BaseRateLimiter = {
    new MockRateLimiter
  }
}

// mock rate limiter that is returned by MockProvider
class MockRateLimiter extends BaseRateLimiter {
  override def maybeSleep(packetSize: Long): Unit = {}
}

