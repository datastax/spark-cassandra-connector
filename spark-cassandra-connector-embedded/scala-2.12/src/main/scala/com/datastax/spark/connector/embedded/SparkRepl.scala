package com.datastax.spark.connector.embedded

import java.io._
import java.net.URLClassLoader

import org.apache.spark.SparkConf
import org.apache.spark.repl.{Main, SparkILoop}

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.GenericRunnerSettings

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

    Main.conf.setAll(conf.getAll)
    val interp = new SparkILoop(Some(in), new PrintWriter(out))
    Main.interp = interp
    val separator = System.getProperty("path.separator")
    val settings = new GenericRunnerSettings(s => throw new RuntimeException(s"Scala options error: $s"))
    settings.processArguments(List("-classpath", paths.mkString(separator)), true)
    interp.process(settings) // Repl starts and goes in loop of R.E.P.L
    Main.interp = null
    Option(Main.sparkContext).foreach(_.stop())
    System.clearProperty("spark.driver.port")
    out.toString
  }

}