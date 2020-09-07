package com.datastax.spark.connector.datasource

import com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.partitioner.CassandraPartition
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._
import org.scalatest.BeforeAndAfterAll

class CassandraCatalogSpecBase
  extends SparkCassandraITFlatSpecBase
    with DefaultCluster
    with BeforeAndAfterAll {

  override def conn: CassandraConnector = CassandraConnector(sparkConf)

  val defaultKs = "catalogtestks"
  val defaultCatalog = "cassandra"

  val EmptyInputPartition = CassandraPartition(0, Array.empty, Iterable.empty, 0)

  def getMetadata() = {
    conn.withSessionDo(_.getMetadata)
  }

  def dropKeyspace(name: String) = {
    conn.withSessionDo(_.execute(s"DROP KEYSPACE IF EXISTS $name"))
  }

  def waitForKeyspaceToExist(keyspace: String, exist: Boolean = true) = {
    eventually(getMetadata().getKeyspace(keyspace).isPresent shouldBe exist)
  }

  def getTable(keyspace: String, table: String): TableMetadata = {
    getMetadata()
      .getKeyspace(fromInternal(keyspace)).get
      .getTable(fromInternal(table)).get
  }

  def createDefaultKs(rf: Int = 5) = {
    dropKeyspace(defaultKs)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $defaultKs WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='$rf')")
    waitForKeyspaceToExist(defaultKs, true)
  }

  implicit val patienceConfig = PatienceConfig(scaled(5 seconds), scaled(200 millis))

  override def beforeClass: Unit = {
    super.beforeClass
    spark.conf.set(s"spark.sql.catalog.$defaultCatalog", classOf[CassandraCatalog].getCanonicalName)
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "cassandra")
  }



}
