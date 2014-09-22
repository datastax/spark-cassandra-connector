package com.datastax.spark.connector.testkit

import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra

/** Basic unit test abstraction. */
trait AbstractSpec extends WordSpecLike with Matchers with BeforeAndAfter

/** Used for IT tests. */
trait SharedEmbeddedCassandra extends EmbeddedCassandra {

  def clearCache(): Unit = CassandraConnector.evictCache()

}

private[connector] object TestEvent {

  case object Stop

  case object Completed

  case class WordCount(word: String, count: Int)

}
