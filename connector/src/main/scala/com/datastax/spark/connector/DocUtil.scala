package com.datastax.spark.connector

import java.nio.file.{FileSystems, Files, Paths}

import com.datastax.spark.connector.util.{ConfigCheck, RefBuilder}

object DocUtil {

  def main(args: Array[String]) {

    val DefaultReferenceFile = Paths.get("..").resolve("doc").resolve("reference.md")

    println("Generating Reference Documentation for Spark Cassandra Conenctor")
    println(s"Found ${ConfigCheck.validStaticProperties.size} Parameters")

    val markdown = RefBuilder.getMarkDown()

    println(s"Generating Reference Documentation for Spark Cassandra Conenctor to ${DefaultReferenceFile.toAbsolutePath}")

    Files.write(DefaultReferenceFile, markdown.getBytes)

  }
}
