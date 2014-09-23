package com.datastax.spark.connector.embedded

import org.apache.spark.{SparkEnv, SparkConf, SparkContext}

trait SparkTemplate {
  val conf = SparkTemplate.conf
  val sc = SparkTemplate.sc
}

object SparkTemplate {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", EmbeddedCassandra.cassandraHost.getHostAddress)
    .set("spark.cleaner.ttl", "3600")
    .setMaster(sys.env.getOrElse("IT_TEST_SPARK_MASTER", "local[*]"))
    .setAppName(getClass.getSimpleName)


  val sc = new SparkContext(conf)

  lazy val actorSystem = SparkEnv.get.actorSystem

}
