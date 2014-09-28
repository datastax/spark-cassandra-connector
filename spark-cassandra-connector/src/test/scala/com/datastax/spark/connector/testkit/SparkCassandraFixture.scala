package com.datastax.spark.connector.testkit

import org.apache.spark.{SparkEnv, SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra

/** Basic unit test abstraction. */
trait AbstractSpec extends WordSpecLike with Matchers with BeforeAndAfter

/** Used for IT tests. */
trait SharedEmbeddedCassandra extends EmbeddedCassandra {

  def clearCache(): Unit = CassandraConnector.evictCache()

}

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


private[connector] object TestEvent {

  case object Stop

  case object Completed

  case class WordCount(word: String, count: Int)

}

