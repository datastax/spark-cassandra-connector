package com.datastax.spark.connector.streaming

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.testkit._
import org.apache.spark.streaming.CheckpointSuite
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.{Matchers, DoNotDiscover, BeforeAndAfterAll}

import scala.util.Random

case class WordCountRow(word:String, count:Long)

@DoNotDiscover
class CheckpointSuiteConnectorConf extends CheckpointSuite{
   override val conf = SparkTemplate
     .defaultConf.clone()
     .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
}

class CheckpointStreamSpec
  extends SparkCassandraITFlatSpecBase
  with Matchers
  with BeforeAndAfterAll
  with SharedEmbeddedCassandra {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  //Stop the default context because the Spark Test Suite will start it's own
  SparkTemplate.sc.stop()

  /**
   * This class comes packaged with an easy to use utility function which tests
   * Checkpointing so lets use it.
   */
  val sparkSuite:CheckpointSuiteConnectorConf = new CheckpointSuiteConnectorConf

  override def beforeAll(){
    CassandraConnector(SparkTemplate.defaultConf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = {'class': " +
        "'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS demo.streaming_join (word TEXT PRIMARY KEY, count" +
        " COUNTER)")
      session.execute("TRUNCATE demo.streaming_join")
      for (d <- dataSeq; word <- d) {
        session.execute("UPDATE demo.streaming_join set count = count + 10 where word = ?", word.trim)
      }
    }
  }

  override def afterAll(): Unit ={
  }

  "Spark Streaming + Checkpointing" should "work with JWCTable and RPCassandra Replica" in {
    val r = new Random()
    val dataTrim = dataSeq.map(_.map(_.trim))

    val dataSets = for ( d <- dataTrim ) yield
      (1 to 200).map( x => d.toList(r.nextInt(d.size))).toSeq

    val repartJoinOpt = (dstream: DStream[String]) => {
         dstream
           .map(Tuple1(_))
           .repartitionByCassandraReplica("demo", "streaming_join")
           .joinWithCassandraTable[WordCountRow]("demo", "streaming_join")
           .map(_._2)
           .map(_.word)
    }

    /**
     * Tests that the dataSets collection is correctly transformed into dataTrim
     */
    sparkSuite.testCheckpointedOperation (
      dataSets,
      repartJoinOpt,
      dataTrim,
      2
      )
  }

}

