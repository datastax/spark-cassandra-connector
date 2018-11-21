package com.datastax.spark.connector.util

import com.datastax.spark.connector.writer.{BaseRateLimiter, LeakyBucketProvider, RateLimiterProvider}

object RateLimiterUtil {
  var provider:RateLimiterProvider = _
  var providerClassName:String = "com.datastax.spark.connector.writer.LeakyBucketProvider"

  // get rate limiter from provider specified by className
  def getRateLimiter(className: String, args: Any*): BaseRateLimiter = {
    setProviderClassName(className)
    println("Getting rate limiter with specified conf from " + provider.getClass.getName)
    provider.getWithConf(args:_*)
  }

  // get standard rate limiter
//  def getRateLimiter(args: Any*): BaseRateLimiter = {
//    println("Getting rate limiter from " + provider.getClass.getName)
//    provider.getWithConf(args:_*)
//  }

//  def setProvider(customProvider: RateLimiterProvider): Unit = {
//    println("Setting rate limiter provider to be " + customProvider.getClass.getName)
//    provider = customProvider
//  }

  private def setProviderClassName(className: String): Unit = {
    providerClassName = className

    try {
      provider = Class.forName(providerClassName).newInstance.asInstanceOf[RateLimiterProvider]
    } catch {
      case e:Exception => println("ERROR: " + e)
    }
  }
}