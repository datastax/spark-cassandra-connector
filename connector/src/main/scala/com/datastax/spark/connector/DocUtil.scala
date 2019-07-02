package com.datastax.spark.connector

import java.io.{File, FileWriter}

import com.datastax.spark.connector.util.{ConfigCheck, RefBuilder}

import scala.util.Try

object DocUtil {

  val DefaultReferenceFile = "doc/reference.md"
  def main(args: Array[String]) {
    println("Generating Reference Documentation for Spark Cassandra Conenctor")
    println(s"Found ${ConfigCheck.validStaticProperties.size} Parameters")

    val markdown = RefBuilder.getMarkDown()
    println(markdown)

    val output = Try(new File(args(0))).getOrElse(new File(DefaultReferenceFile))
    println(s"Generating Reference Documentation for Spark Cassandra Conenctor to $output")

    val fb = new FileWriter(output)
    fb.write(RefBuilder.getMarkDown())
    fb.close()
  }
}
