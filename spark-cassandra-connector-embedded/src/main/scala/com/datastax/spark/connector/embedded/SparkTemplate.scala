package com.datastax.spark.connector.embedded

import java.net.InetAddress

import org.apache.spark.{SparkEnv, SparkConf, SparkContext}

trait SparkTemplate {
  val conf = SparkTemplate.conf
}

object SparkTemplate {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", EmbeddedCassandra.getHost(0).getHostAddress)
    .set("spark.cleaner.ttl", "3600")
    .set("spark.metrics.conf", this.getClass.getResource("/metrics.properties").getPath)
    .setMaster(sys.env.getOrElse("IT_TEST_SPARK_MASTER", "local[*]"))
    .setAppName(getClass.getSimpleName)

  System.err.println("The Spark configuration is as follows:\n" + conf.toDebugString)

  lazy val actorSystem = SparkEnv.get.actorSystem

}
