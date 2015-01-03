package com.datastax.spark.connector.util

import scala.util.Random

/** Benchmarks a simulation of a heap-based stream-sort.
  * In each iteration, a random item is added to the priority queue
  * and the head item is removed. */
object PriorityHashMapBenchmark extends App {

  val capacity = 1024 * 8
  val count = 1024 * 8
  val random = new Random

  val m = new PriorityHashMap[Int, Int](capacity)

  // we need to measure how much the loop and random alone are taking
  println("Benchmarking caller...")
  BenchmarkUtil.timeIt {
    for (i <- 1 to 1000000) {  // 1 million
      random.nextInt(count).asInstanceOf[AnyRef]
      random.nextInt().asInstanceOf[AnyRef]
    }
  }

  println("Benchmarking real code...")
  BenchmarkUtil.timeIt {
    for (i <- 1 to 1000000) {  // 1 million
      if (m.size >= count)
        m.remove(m.headKey)
      m.put(random.nextInt(count), random.nextInt(count))
    }
  }
}
