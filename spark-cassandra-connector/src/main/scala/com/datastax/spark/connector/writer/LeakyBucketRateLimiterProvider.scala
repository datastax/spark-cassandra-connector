package main.scala.com.datastax.spark.connector.writer

import com.datastax.spark.connector.util.Logging
import com.datastax.spark.connector.writer.{BaseRateLimiter, LeakyBucketRateLimiter, RateLimiterProvider}

/**
  * Instantiates a leaky bucket rate limiter based on the supplied configuration.
  */
class LeakyBucketRateLimiterProvider extends RateLimiterProvider with Logging {
  {}

  override def getRateLimiterWithConf(args: Any*): BaseRateLimiter = {
    val rate = args(0).asInstanceOf[Number].longValue
    val bucketSize = args(1).asInstanceOf[Number].longValue

    /**
      * If optional arguments are present and cannot be casted correctly,
      * omit them and instantiate rate limiter with only rate and bucketSize
      */
    try {
      if (args.size > 2) {
        val time = args(2).asInstanceOf[() => Long]
        if (args.size > 3) {
          val sleep = args(3).asInstanceOf[Long => Any]
          new LeakyBucketRateLimiter(rate, bucketSize, time, sleep)
        }
        new LeakyBucketRateLimiter(rate, bucketSize, time)
      }
    } catch {
      case _: Exception => {
        logError("Invalid optional arguments when instantiating leaky bucket rate limiter")
        new LeakyBucketRateLimiter(rate, bucketSize)
      }
    }

    new LeakyBucketRateLimiter(rate, bucketSize)
  }
}
