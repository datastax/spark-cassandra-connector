package com.datastax.spark.connector.writer

trait RateLimiterProvider {
  def getWithConf(args: Any*): BaseRateLimiter
}

class LeakyBucketProvider extends RateLimiterProvider {
  override def getWithConf(args: Any*): BaseRateLimiter = {
    require(args.length >= 2)

    val rate= args(0) match {
      case x:Int => x.intValue()
      case x:Long => x.longValue()
    }

    val bucketSize = args(1) match {
      case x:Int => x.intValue()
      case x:Long => x.longValue()
    }

    val time:() => Long = if (args.length >= 3) args(2).asInstanceOf[() => Long] else System.currentTimeMillis
    val sleep:Long => Any = if (args.length >= 4) args(3).asInstanceOf[Long => Any] else Thread.sleep

    new LeakyBucketRateLimiter(rate, bucketSize, time, sleep)
  }
}
