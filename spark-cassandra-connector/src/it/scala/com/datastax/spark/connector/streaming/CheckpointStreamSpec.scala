package com.datastax.spark.connector.streaming

import scala.util.Random

import org.apache.spark.streaming.CheckpointSuite
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Matchers}

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.testkit._

case class WordCountRow(word: String, count: Long)

@DoNotDiscover
class CheckpointSuiteConnectorConf extends CheckpointSuite {
  override val conf = SparkTemplate.defaultConf
    .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
}

class CheckpointStreamSpec
  extends SparkCassandraITFlatSpecBase
  with Matchers
  with BeforeAndAfterAll
  with SharedEmbeddedCassandra {

  useCassandraConfig(Seq(YamlTransformations.Default))
  //Stop the default context because the Spark Test Suite will start it's own
  useSparkConf(null)

  /**
    * This class comes packaged with an easy to use utility function which tests
    * Checkpointing so lets use it.
    */
  val sparkSuite: CheckpointSuiteConnectorConf = new CheckpointSuiteConnectorConf
  CassandraConnector(sparkSuite.conf).withSessionDo { session =>
    createKeyspace(session)
    session.execute(s"CREATE TABLE $ks.streaming_join (word TEXT PRIMARY KEY, count COUNTER)")
    for (d <- dataSeq; word <- d) {
      session.execute(s"UPDATE $ks.streaming_join set count = count + 10 where word = ?", word.trim)
    }
  }

  "Spark Streaming + Checkpointing" should "work with JWCTable and RPCassandra Replica" ignore {
    val r = new Random()
    val dataTrim = dataSeq.map(_.map(_.trim))

    val dataSets = for (d <- dataTrim) yield
      (1 to 50).map(x => d.toList(r.nextInt(d.size))).toSeq

    val repartJoinOpt = (dstream: DStream[String]) => {
      val joined = dstream
        .map(Tuple1(_))
        .repartitionByCassandraReplica(ks, "streaming_join")
        .joinWithCassandraTable[WordCountRow](ks, "streaming_join")
        .map(_._2)
        .map(_.word)
      joined
    }

    sparkSuite.beforeFunction()
    try {
      // Tests that the dataSets collection is correctly transformed into dataTrim
      println("DataSets: " + dataSets)
      println("Expeced : " + dataTrim)
      // TODO: fix this test
//      sparkSuite.testCheckpointedOperation(dataSets, repartJoinOpt, dataTrim, 2)
    } finally {
      sparkSuite.afterFunction()
    }
  }

}

