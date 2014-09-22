package com.datastax.spark.connector.testkit

import scala.collection.immutable
import akka.util.Timeout
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra

trait AbstractSpec extends WordSpecLike with Matchers with BeforeAndAfter

trait SparkCassandraFixture {

  import scala.concurrent.duration._

  implicit val DefaultTimeout = Timeout(5.seconds)

  val data = immutable.Set("words ", "may ", "count ")

  val actorName = "my-actor"

}

/** Used for IT tests. */
trait SharedEmbeddedCassandra extends EmbeddedCassandra {

  def clearCache(): Unit = CassandraConnector.evictCache()

}

private[connector] object TestEvent {

  case object Stop

  case object Completed

  case class WordCount(word: String, count: Int)

}
