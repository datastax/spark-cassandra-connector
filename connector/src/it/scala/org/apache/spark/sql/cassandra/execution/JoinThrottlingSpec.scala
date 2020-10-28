package org.apache.spark.sql.cassandra.execution

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation.DirectJoinSettingParam
import org.apache.spark.sql.cassandra._
import org.scalatest.concurrent.Eventually

import scala.concurrent.Future

class JoinThrottlingSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Eventually {

  override lazy val conn = CassandraConnector(defaultConf)

  private val rowsCount = 10000

  override def beforeClass {
    spark.conf.set(DirectJoinSettingParam.name, "auto")
    conn.withSessionDo { session =>
      val executor = getExecutor(session)
      createKeyspace(session)
      awaitAll(
        Future {
          session.execute(s"CREATE TABLE IF NOT EXISTS $ks.kvtarget (k int PRIMARY KEY, v int, id int)")
          session.execute(s"CREATE TABLE IF NOT EXISTS $ks.kv (k int PRIMARY KEY, v int)")
          val ps = session.prepare(s"INSERT INTO $ks.kv (k,v) VALUES (?,?)")
          awaitAll {
            for (id <- 1 to rowsCount) yield {
              executor.executeAsync(ps.bind(id: java.lang.Integer, id: java.lang.Integer))
            }
          }
        }
      )
      executor.waitForCurrentlyExecutingTasks()
    }
  }

  private def timed(measureUnit: => Unit): Long = {
    val startMillis = System.currentTimeMillis()
    measureUnit
    System.currentTimeMillis() - startMillis
  }

  private def joinWithRowsPerSecondThrottle(rowsPerSecondPerCore: Int): Int = {
    import spark.implicits._
    val right = spark.range(1, rowsCount).map(_.intValue)
      .withColumnRenamed("value", "id")
    val left = spark.read.cassandraFormat("kv", ks.toLowerCase)
      .option(ReadConf.ReadsPerSecParam.name, rowsPerSecondPerCore)
      .load()
    val join = left.join(right, left("k") === right("id"))

    val durationMillis = timed {
      join.write.format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> ks, "table" -> "kvtarget"))
        .mode("append")
        .save()
    }

    val durationSeconds = durationMillis.toInt / 1000
    val minimalDurationSeconds = rowsCount / rowsPerSecondPerCore / SparkTemplate.DefaultParallelism
    withClue(s"The expected duration of this join operation should not be shorter then $minimalDurationSeconds " +
      s"for rowsPerSecondPerCore=$rowsPerSecondPerCore.") {
      durationSeconds should be >= minimalDurationSeconds
    }
    durationSeconds
  }

  /* SPARKC-627 */
  it should "throttle join by rows per second" in {
    val slowJoinDuration = joinWithRowsPerSecondThrottle(800)
    val fastJoinDuration = joinWithRowsPerSecondThrottle(1600)

    withClue("Increasing rows per second throttle parameter should result in lowering the execution time") {
      fastJoinDuration should be < slowJoinDuration
    }
  }
}
