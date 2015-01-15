package com.datastax.spark.connector.util

object SpanningIteratorBenchmark extends App {

  val iterator = Iterator.from(0)
  val groupsOf1 = new SpanningIterator(iterator, (i: Int) => i)
  val groupsOf10 = new SpanningIterator(iterator, (i: Int) => i / 10)
  val groupsOf1000 = new SpanningIterator(iterator, (i: Int) => i / 1000)

  println("1,000,000 groups of size 1 per each iteration:")
  BenchmarkUtil.timeIt {
    groupsOf10.drop(1000000)
  }

  println("100,000 groups of size 10 per each iteration:")
  BenchmarkUtil.timeIt {
    groupsOf10.drop(100000)
  }

  println("1000 groups of size 1000, per each iteration:")
  BenchmarkUtil.timeIt {
    groupsOf1000.drop(1000)
  }

}
