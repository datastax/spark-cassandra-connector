package com.datastax.spark.connector.embedded

import java.io.{BufferedReader, PrintWriter, StringReader, StringWriter}
import java.net.URLClassLoader

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.repl.{Main, SparkILoop}

trait SparkRepl {

  def runInterpreter(input: String): String = {
    SparkTemplate.defaultConf.getAll.filter(_._1.startsWith("spark.cassandra."))
      .foreach(p => System.setProperty(p._1, p._2))
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

    val interp = new SparkILoop(Some(in), new PrintWriter(out))
    Main.interp = interp
    // due to some mysterious compilation error classServer needs to be accessed via reflection
    //      Main.classServer.start()
    Main.asInstanceOf[AnyRef] match {
      case x: {def classServer: {def start(): Unit}} => x.classServer.start()
    }
    val separator = System.getProperty("path.separator")
    org.apache.spark.repl.Main.s.processArguments(List("-classpath", paths.mkString(separator)), true)
    interp.process(Main.s)
    Main.asInstanceOf[AnyRef] match {
      case x: {def classServer: {def stop(): Unit}} => x.classServer.stop()
    }
    //      Main.classServer.stop()
    Main.interp = null
    Option(Main.sparkContext).foreach(_.stop())
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    out.toString
  }

}