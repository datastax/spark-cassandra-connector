package com.datastax.spark.connector

import java.util.concurrent.Executors

import com.datastax.oss.driver.api.core.{CqlSession, ProtocolVersion}
import com.datastax.spark.connector.cluster.ClusterProvider
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.testkit.AbstractSpec
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

trait SparkCassandraITFlatSpecBase extends FlatSpec with SparkCassandraITSpecBase {
  override def report(message: String): Unit = info
}

trait SparkCassandraITWordSpecBase extends WordSpec with SparkCassandraITSpecBase {
}

trait SparkCassandraITAbstractSpecBase extends AbstractSpec with SparkCassandraITSpecBase {
}

trait SparkCassandraITSpecBase
  extends Suite
  with Matchers
  with BeforeAndAfterAll
  with ClusterProvider {

  final def defaultConf: SparkConf = {
    SparkTemplate.defaultConf
      .setAll(cluster.connectionParameters)
  }
  final def sparkConf = defaultConf

  lazy val spark = SparkSession.builder().config(sparkConf).getOrCreate().newSession()
  lazy val sparkSession = spark
  lazy val sc = spark.sparkContext

  val originalProps = sys.props.clone()

  final override def beforeAll(): Unit = {
    initHiveMetastore()
    beforeClass()
  }

  def beforeClass(): Unit = {}

  def afterClass(): Unit = {}

  final override def afterAll(): Unit = {
    afterClass()
    restoreSystemProps()
  }

  def getKsName = {
    val className = this.getClass.getSimpleName
    val suffix = StringUtils.splitByCharacterTypeCamelCase(className.filter(_.isLetterOrDigit)).mkString("_")
    s"test_$suffix".toLowerCase()
  }

  def conn: CassandraConnector = ???

  def initHiveMetastore() {
    /**
      * Creates CassandraHiveMetastore
      */
    val conn = CassandraConnector(sparkConf)
    conn.withSessionDo { session =>
      session.execute(
        """
          |CREATE KEYSPACE IF NOT EXISTS "HiveMetaStore" WITH REPLICATION =
          |{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }; """
          .stripMargin)
      session.execute(
        """CREATE TABLE IF NOT EXISTS "HiveMetaStore"."sparkmetastore"
          |(key text,
          |entity text,
          |value blob,
          |PRIMARY KEY (key, entity))""".stripMargin)
    }
  }

  def pv = conn.withSessionDo(_.getContext.getProtocolVersion)

  def report(message: String): Unit = {}

  val ks = getKsName

  def skipIfProtocolVersionGTE(protocolVersion: ProtocolVersion)(f: => Unit): Unit = {
    if (!(pv.getCode >= protocolVersion.getCode)) f
    else report(s"Skipped Because ProtcolVersion $pv >= $protocolVersion")
  }

  def skipIfProtocolVersionLT(protocolVersion: ProtocolVersion)(f: => Unit): Unit = {
    if (!(pv.getCode < protocolVersion.getCode)) f
    else report(s"Skipped Because ProtocolVersion $pv < $protocolVersion")
  }

  implicit val ec = SparkCassandraITSpecBase.ec

  def awaitAll(units: Future[Unit]*): Unit = {
    Await.result(Future.sequence(units), Duration.Inf)
  }

  def keyspaceCql(name: String = ks) =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $name
       |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
       |AND durable_writes = false
       |""".stripMargin

  def createKeyspace(session: CqlSession, name: String = ks): Unit = {
    session.execute(s"DROP KEYSPACE IF EXISTS $name")
    session.execute(keyspaceCql(name))
  }

  /**
    * Ensures that the tables exist in the metadata object for this session. This can be
    * an issue with some schema debouncing.
    */
  def awaitTables(tableNames: String*): Unit = {
    eventually(timeout(Span(2, Seconds))) {
      conn.withSessionDo(session =>
        session
          .getMetadata
          .getKeyspace(ks).get()
          .getTables().keySet()
          .containsAll(tableNames.asJava)
      )
    }
  }


  def restoreSystemProps(): Unit = {
    sys.props ++= originalProps
    sys.props --= (sys.props.keySet -- originalProps.keySet)
  }

}

object SparkCassandraITSpecBase {
  val executor = Executors.newFixedThreadPool(100)
  val ec = ExecutionContext.fromExecutor(executor)
}
