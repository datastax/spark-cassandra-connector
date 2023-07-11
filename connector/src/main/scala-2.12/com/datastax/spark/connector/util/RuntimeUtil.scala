package com.datastax.spark.connector.util

import org.apache.spark.repl.SparkILoop
import scala.tools.nsc.Settings
import java.io.{BufferedReader, PrintWriter}
import scala.collection.parallel.ParIterable

class Scala213SparkILoop(in: BufferedReader, out: PrintWriter) extends SparkILoop(in, out) {

  def run(interpreterSettings: Settings): Boolean = {
    super.process(interpreterSettings)
  }
}


object RuntimeUtil {

  def toParallelIterable[A](iterable: Iterable[A]): ParIterable[A] = {
    iterable.par
  }

  def createSparkILoop(in: BufferedReader, out: PrintWriter): Scala213SparkILoop = {
    new Scala213SparkILoop(in, out)
  }
}
