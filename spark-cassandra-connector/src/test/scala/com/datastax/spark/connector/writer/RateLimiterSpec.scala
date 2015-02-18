package com.datastax.spark.connector.writer

import org.scalatest.{FlatSpec, Matchers}

class RateLimiterSpec extends FlatSpec with Matchers {

  // Warning, this test is timing-based.
  // It may fail if run on a heavily loaded system with unpredictable performance.
  // Increase allowedSystemInducedPause if you encounter any problems:
  val allowedSystemInducedPause = 250 // ms

  "RateLimiter" should "not cause delays if rate is not exceeded" in {
    val limiter = new RateLimiter(Long.MaxValue, 1000)
    val start1 = System.currentTimeMillis()
    for (i <- 1 to 1000000)
      limiter.maybeSleep(0)
    val end1 = System.currentTimeMillis()
    val referenceElapsed = end1 - start1

    val start2 = System.currentTimeMillis()
    for (i <- 1 to 1000000)
      limiter.maybeSleep(1000)
    val end2 = System.currentTimeMillis()
    val testElapsed = end2 - start2
    testElapsed should be <= referenceElapsed + allowedSystemInducedPause

  }

  it should "sleep to not exceed the target rate" in {
    // 10 units per second + 5 units burst allowed
    val bucketSize = 5
    val rate = 10
    val limiter = new RateLimiter(rate, bucketSize)

    val start = System.currentTimeMillis()
    val iterations = 25
    for (i <- 1 to iterations)
      limiter.maybeSleep(1)
    val end = System.currentTimeMillis()

    val elapsedMillis = end - start
    val expectedRunTime = (iterations - bucketSize) * 1000L / rate
    elapsedMillis should be > expectedRunTime
    elapsedMillis should be < expectedRunTime + allowedSystemInducedPause
  }

}
