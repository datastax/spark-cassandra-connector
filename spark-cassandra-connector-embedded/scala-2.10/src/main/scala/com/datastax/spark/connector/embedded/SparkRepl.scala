package com.datastax.spark.connector.embedded

import java.io._
import java.net.URLClassLoader

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.repl.SparkILoop

object SparkRepl {

  def runInterpreter(input: String, conf: SparkConf): String = {
    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()
    val cl = getClass.getClassLoader
    var paths = new ArrayBuffer[String]
    cl match {
      case urlLoader: URLClassLoader =>
        for (url <- urlLoader.getURLs) {
          if (url.getProtocol == "file") {
            paths += url.getFile
          }
        }
      case _ =>
    }

    sys.props ++= conf.getAll
    val interp = new SparkILoop(in, new PrintWriter(out))
    org.apache.spark.repl.Main.interp = interp
    val separator = System.getProperty("path.separator")
    interp.process(Array("-classpath", paths.mkString(separator)))
    org.apache.spark.repl.Main.interp = null
    if (interp.sparkContext != null) {
      interp.sparkContext.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    out.toString
  }

}
