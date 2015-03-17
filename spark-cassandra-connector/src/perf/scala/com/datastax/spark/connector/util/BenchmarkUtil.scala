package com.datastax.spark.connector.util

import scala.annotation.tailrec

/** Utilities for benchmarking code */
object BenchmarkUtil {

  private val sampleSize = 5
  private val minTime = 1000000000  // 1s

  def formatTime(time: Double): String = {
    if (time < 1e-7)
      f"${time * 1000000000.0}%.2f ns"
    else if (time < 1e-4)
      f"${time * 1000000.0}%.2f us"
    else if (time < 0.1)
      f"${time * 1000.0}%.2f ms"
    else
      f"$time%.3f s"
  }

  @inline
  private def time(loops: Long)(code: => Any): Long = {
    val start = System.nanoTime()
    var counter = 0L
    while (counter < loops) {
      code
      counter += 1L
    }
    val end = System.nanoTime()
    end - start
  }

  @inline
  private def timeN(loops: Long)(code: => Any): Long = {
    var i = 0
    var bestTime = Long.MaxValue
    while (bestTime >= minTime && i < sampleSize) {
      val t = time(loops)(code)
      if (t < bestTime)
        bestTime = t
      i += 1
    }
    bestTime
  }

  /** Runs the given code multiple times and prints how long it took. */
  @tailrec
  @inline
  def timeIt[T](loops: Long)(code: => T): T = {
    val bestTime = timeN(loops)(code)
    if (bestTime < minTime)
      timeIt(loops * 10)(code)
    else {
      val timePerLoop = bestTime / loops
      printf("loops: %d, time per loop: %s, loops/s: %.3f\n",
        loops, formatTime(timePerLoop / 1000000000.0), 1000000000.0 / timePerLoop)
      code
    }
  }

  @inline
  def timeIt[T](code: => T): T =
    timeIt(1)(code)


  def printTime[T](message: String)(code: => T): T = {
    val start = System.nanoTime()
    val result = code
    val end = System.nanoTime()
    println(message + ": " + formatTime((end - start) / 1000000000.0))
    result
  }

  def printTime[T](code: => T): T =
    printTime("elapsed")(code)
}
