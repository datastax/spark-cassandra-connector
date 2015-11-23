package com.datastax.spark.connector

import java.io.{File, FileWriter}

import scala.util.Try

import com.datastax.spark.connector.util.{ConfigCheck, PropertiesFileBuilder}

object PropertiesFileUtil {

  val DefaultPropertiesFile = "spark-defaults.conf"

  def main(args: Array[String]) {
    println("Generating Reference properties file for Spark Cassandra Connector")
    println(s"Found ${ConfigCheck.validStaticProperties.size} Parameters")

    val markdown = PropertiesFileBuilder.getPropertiesFileContent()
    println(markdown)

    val output = Try(new File(args(0))).getOrElse(new File(DefaultPropertiesFile))
    println(s"Generating Reference properties file for Spark Cassandra Connector to $output")

    val fb = new FileWriter(output)
    fb.write(PropertiesFileBuilder.getPropertiesFileContent())
    fb.close()
  }
}
