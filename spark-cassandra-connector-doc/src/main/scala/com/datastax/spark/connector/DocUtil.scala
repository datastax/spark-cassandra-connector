package com.datastax.spark.connector

import com.datastax.spark.connector.util.{RefBuilder, ConfigCheck}

import scala.io.Source
import java.io.{FileWriter, File, PrintWriter}
import scala.util.Try

object DocUtil {
  def main(args: Array[String]) {
    println("Generating Reference Documentation for Spark Cassandra Conenctor")
    println(s"Found ${ConfigCheck.validStaticProperties.size} Parameters")

    val markdown = RefBuilder.getMarkDown()
    println(markdown)

    val output = Try(new File(args(0))).getOrElse(new File("doc/reference.md"))
    val fb = new FileWriter(output)
    fb.write(RefBuilder.getMarkDown())
    fb.close()
  }
}
