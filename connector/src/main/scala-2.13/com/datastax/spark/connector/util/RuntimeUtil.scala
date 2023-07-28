package com.datastax.spark.connector.util

import org.apache.spark.repl.SparkILoop

import java.io.{BufferedReader, PrintWriter}
import scala.collection.parallel.ParIterable


object RuntimeUtil {

  def toParallelIterable[A](iterable: Iterable[A]): ParIterable[A] = {
    import scala.collection.parallel.CollectionConverters._
    iterable.par
  }

  def createSparkILoop(in: BufferedReader, out: PrintWriter): SparkILoop = {
    new SparkILoop(in, out)
  }
}
