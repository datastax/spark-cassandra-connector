package com.datastax.spark.connector.util

import com.datastax.spark.connector.writer.{BaseRateLimiter, RateLimiterProvider}

/**
  * Exports a method to retrieve a custom rate limiter based on dynamic configuration.
  */
object RateLimiterUtil extends Logging {
  var provider:RateLimiterProvider = _

  /**
    * Instantiates a rate limiter provider based on its fully qualified classname and should that not be possible,
    * fallbacks to the leaky bucket rate limiter provider in this project.
    *
    * @param className fully qualified classname of the rate limiter provider to instantiate
    * @param args optional sequence of arguments passed on to the provider
    * @return an instantiated rate limiter
    */
  def getRateLimiter(className: String, args: Any*): BaseRateLimiter = {
    try {
      provider = Class.forName(className).newInstance.asInstanceOf[RateLimiterProvider]
    } catch {
      case e:ClassNotFoundException => {
        logError("Could not find custom rate limiter provider. Error: " + e)
        throw e
      }
      case e:InstantiationException => {
        logError("Could not instantiate custom rate limiter provider. Error: " + e)
        throw e
      }
      case e:Throwable => {
        logError("Error: " + e)
        throw e
      }
    }

    provider.getRateLimiterWithConf(args:_*)
  }
}