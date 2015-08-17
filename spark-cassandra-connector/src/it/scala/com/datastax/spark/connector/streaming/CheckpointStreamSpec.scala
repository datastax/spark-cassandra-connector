package com.datastax.spark.connector.streaming

import java.io.{PrintWriter, BufferedWriter, OutputStreamWriter}
import java.net.{InetAddress, ServerSocket, Socket}

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.streaming.StreamingEvent.ReceiverStarted
import com.datastax.spark.connector.testkit._
import org.apache.spark.SparkEnv
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar._


import scala.reflect.io.Path
import scala.util.{Random, Try}

class CheckpointStreamSpec extends StreamingSpec with TimeLimitedTests {

  val timeLimit = 20 seconds

  /* Initializations - does not work in the actor test context in a static before() */
  CassandraConnector(SparkTemplate.defaultConf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = {'class': " +
      "'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS demo.streaming_join (word TEXT PRIMARY KEY, count" +
      " COUNTER)")
    for (d <- dataSeq; word <- d) {
      session.execute("UPDATE demo.streaming_join set count = count + 10 where word = ?", word.trim)
    }
    session.execute("CREATE TABLE IF NOT EXISTS demo.checkpoint_wordcount (word TEXT PRIMARY KEY," +
      " count COUNTER)")
    session.execute("CREATE TABLE IF NOT EXISTS demo.checkpoint_output (word TEXT PRIMARY KEY, " +
      "count COUNTER)")
    session.execute("TRUNCATE demo.checkpoint_output")
    session.execute("TRUNCATE demo.checkpoint_wordcount")
  }

  "actorStream" must {

    "work with JWCTable and RPCassandra Replica with checkpointing" in {

      val ipAddress = "localhost"
      val port = 39400

      val checkpointDirName = "/tmp/checkpoint-streaming-dir"
      val checkpointDir = "file:///tmp/checkpoint-streaming-dir"
      Try(Path(checkpointDirName).deleteRecursively())

      /** Passed into Checkpoint Test **/
      def getContextForCheckpointTest(): StreamingContext = {
        val ssc = new StreamingContext(sc, Milliseconds(800))
        info("Creating Context")
        val stream = ssc.socketTextStream(ipAddress, port, StorageLevel.MEMORY_AND_DISK)
          .flatMap(_.split("\\s+"))

        val wc = stream
          .map(x => (x, 1))
          .reduceByKey(_ + _)

        wc.saveToCassandra("demo", "checkpoint_wordcount")

        val jcRepart = wc
          .repartitionByCassandraReplica("demo", "streaming_join")

        jcRepart
          .joinWithCassandraTable("demo", "streaming_join")
          .map(_._2)
          .saveToCassandra("demo", "checkpoint_output")

        ssc.checkpoint(checkpointDir)
        ssc
      }


      val checkPointOptions = Some((getContextForCheckpointTest: () => StreamingContext,
        checkpointDir))

      for ((dataSet, index) <- dataSeq.zipWithIndex) {

        val serverSocket = Try(new ServerSocket(port, 10, InetAddress.getByName(null)))
          .getOrElse(fail("unable to setup server socket"))

        val clientThread = new Thread(new Runnable {
          def run(): Unit = {
            val socket = Try(serverSocket.accept())
              .getOrElse(fail("Unable to setup socket stream"))

            val osw = new PrintWriter(socket.getOutputStream(), true);
            for (it <- 1 to 4) {
              osw
                .println(
                  (1 to 100)
                    .map(_ =>
                    dataSet.toSeq(Random.nextInt(dataSet.size))
                    ).mkString
                )
              osw.flush()
              Thread.sleep(400)
            }
          }
        })

        clientThread.start()

        withStreamingContext(
        { ssc =>
          info(s"Iteration $index")

          ssc.start()
          clientThread.join()
          val result = ssc.cassandraTable[(String, Int)]("demo", "checkpoint_wordcount")
            .map(_._1)
            .collect
          val output = ssc.cassandraTable[(String, Int)]("demo", "checkpoint_wordcount")
            .map(_._1)
            .collect

          for (element <- dataSet)
            result should contain(element.trim)
        }
        ,
        checkPointOptions)

        serverSocket.close()
      }
    }
  }
}

