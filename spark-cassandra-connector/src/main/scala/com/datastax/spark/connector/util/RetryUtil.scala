package com.datastax.spark.connector.util

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object RetryUtil {

  @tailrec
  def TryUntilValid[T]( f: => T, tries: Int = 3, sleepBetweenTriesInMS: Long = 1000): T = {
    Try{ f } match {
      case Success(x) => x
      case _ if tries > 1 => {
        Thread.sleep(sleepBetweenTriesInMS)
        TryUntilValid(f, tries - 1, sleepBetweenTriesInMS)
      }
      case Failure(x) => throw x
    }
  }


}
