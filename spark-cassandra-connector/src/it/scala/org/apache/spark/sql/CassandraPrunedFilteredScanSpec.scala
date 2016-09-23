package org.apache.spark.sql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.YamlTransformations
import com.datastax.spark.connector.rdd.{CassandraTableScanRDD, CqlWhereClause}
import com.datastax.spark.connector.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution._

import scala.concurrent.Future

class CassandraPrunedFilteredScanSpec extends SparkCassandraITFlatSpecBase with Logging  {
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  override val conn = CassandraConnector(defaultConf)

  val cassandraFormat = "org.apache.spark.sql.cassandra"

  override def beforeAll(): Unit = {
    conn.withSessionDo { session =>
      createKeyspace(session)

      awaitAll(
        Future {
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.colors
                |(name TEXT, color TEXT, priority INT, PRIMARY KEY (name, priority)) """
                .stripMargin)
        },
        Future {
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $ks.fields
                |(k INT, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, PRIMARY KEY (k)) """
                .stripMargin)
        }
      )
    }
  }

  val colorOptions = Map("keyspace" -> ks, "table" -> "colors")
  val fieldsOptions = Map("keyspace" -> ks, "table" -> "fields")
  val withPushdown = Map("pushdown" -> "true")
  val withoutPushdown = Map("pushdown" -> "false")

  "CassandraPrunedFilteredScan" should "pushdown predicates for clustering keys" in {
    val colorDF = sparkSession.read.format(cassandraFormat).options(colorOptions ++ withPushdown).load()
    val executionPlan = colorDF.filter("priority > 5").queryExecution.executedPlan
    val cts = findCassandraTableScanRDD(executionPlan)
    cts.isDefined shouldBe true
    cts.get.where shouldBe CqlWhereClause(Seq(""""priority" > ?"""), List(5))
  }

  it should "not pushdown predicates for clustering keys if filterPushdown is disabled" in {
    val colorDF = sparkSession.read.format(cassandraFormat).options(colorOptions ++ withoutPushdown).load()
    val executionPlan = colorDF.filter("priority > 5").queryExecution.executedPlan
    val cts = findCassandraTableScanRDD(executionPlan)
    cts.isDefined shouldBe true
    cts.get.where shouldBe CqlWhereClause(Seq(), List())
  }

  it should "prune data columns" in {
    val fieldsDF = sparkSession.read.format(cassandraFormat).options(fieldsOptions ++ withPushdown).load()
    val executionPlan = fieldsDF.select("b","c","d").queryExecution.executedPlan
    val cts = findCassandraTableScanRDD(executionPlan)
    cts.isDefined shouldBe true
    cts.get.selectedColumnNames should contain theSameElementsAs Seq("b", "c", "d")
  }

  it should "prune data columns if filterPushdown is disabled" in {
    val fieldsDF = sparkSession.read.format(cassandraFormat).options(fieldsOptions ++ withoutPushdown).load()
    val executionPlan = fieldsDF.select("b","c","d").queryExecution.executedPlan
    val cts = findCassandraTableScanRDD(executionPlan)
    cts.isDefined shouldBe true
    cts.get.selectedColumnNames should contain theSameElementsAs Seq("b", "c", "d")
  }

  def findCassandraTableScanRDD(sparkPlan: SparkPlan): Option[CassandraTableScanRDD[_]] = {
    def _findCassandraTableScanRDD(rdd: RDD[_]): Option[CassandraTableScanRDD[_]] = {
      rdd match {
        case ctsrdd: CassandraTableScanRDD[_] => Some(ctsrdd)
        case other: RDD[_] => other.dependencies.iterator
            .flatMap(dep => _findCassandraTableScanRDD(dep.rdd)).take(1).toList.headOption
      }
    }

    sparkPlan match {
      case prdd: RDDScanExec => _findCassandraTableScanRDD(prdd.rdd)
      case prdd: RowDataSourceScanExec => _findCassandraTableScanRDD(prdd.rdd)
      case filter: FilterExec => findCassandraTableScanRDD(filter.child)
      case wsc: WholeStageCodegenExec => findCassandraTableScanRDD(wsc.child)
      case _ => None
    }
  }

}