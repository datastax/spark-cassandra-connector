package com.datastax.spark.connector.writer

import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, Matchers}


class LeakyBucketRateLimiterSpec extends FlatSpec with Matchers with MockFactory with Eventually{

 val TestRates = Seq(1L, 2L, 4L, 6L, 8L, 16L, 32L, WriteConf.ThroughputMiBPSParam.default.toLong)

  "RateLimiter" should "not cause delays if rate is not exceeded" in {
    var now: Long = 0
    val sleep = mockFunction[Long, Any]("sleep")
    sleep.expects(*).never()

    val limiter = new LeakyBucketRateLimiter(Long.MaxValue, 1000, () => now, sleep)
    for (i <- 1 to 1000000) {
      now += 1
      limiter.maybeSleep(1000)
    }
  }

  it should "sleep to not exceed the target rate" in {
    var now: Long = 0
    var sleepTime: Long = 0

    def sleep(delay: Long) = {
      sleepTime += delay
      now += delay
    }

    // 10 units per second + 5 units burst allowed
    val bucketSize = 5
    val rate = 10
    val limiter = new LeakyBucketRateLimiter(rate, bucketSize, () => now, sleep)

    val iterations = 25
    for (i <- 1 to iterations)
      limiter.maybeSleep(1)

    sleepTime should be((iterations - bucketSize) * 1000L / rate)
  }

  it should "sleep and leak properly with different Rates" in {
    for (rate <- TestRates) {
      val bucketSize = rate * 2
      var now: Long = 0
      var sleepTime: Long = 0

      def sleep(delay: Long) = {
        sleepTime += delay
        now += delay
      }

      val limiter = new LeakyBucketRateLimiter(rate, rate * 2, () => now, sleep)
      for (leakNum <- 1 to 1000) {
        assert(
          limiter.bucketFill.get() >= 0,
          "bucketFill has been overflowed, or has had a large negative number added to it")
        limiter.maybeSleep(rate)
      }

      eventually {
        limiter.leak()
        val delay = (limiter.bucketFill.get() - bucketSize) * 1000 / rate
        assert(delay <= 0, "Rate limiter was unable to leak it's way back to 0 delay")
      }
      sleepTime should not be (0)
    }
  }

}
