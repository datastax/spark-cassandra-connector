package com.datastax.spark.connector

import java.util.concurrent.Executors

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import org.apache.commons.lang3.StringUtils
import org.scalatest._
import com.datastax.driver.core.{ProtocolVersion, Session}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate}
import com.datastax.spark.connector.testkit.{AbstractSpec, SharedEmbeddedCassandra}
import com.datastax.spark.connector.util.SerialShutdownHooks


trait SparkCassandraITFlatSpecBase extends FlatSpec with SparkCassandraITSpecBase {
  override def report(message: String): Unit = info
}

trait SparkCassandraITWordSpecBase extends WordSpec with SparkCassandraITSpecBase

trait SparkCassandraITAbstractSpecBase extends AbstractSpec with SparkCassandraITSpecBase

trait SparkCassandraITSpecBase extends Suite with Matchers with SharedEmbeddedCassandra with SparkTemplate with BeforeAndAfterAll {

  val originalProps = sys.props.clone()

  def getKsName = {
    val className = this.getClass.getSimpleName
    val suffix =  StringUtils.splitByCharacterTypeCamelCase(className.filter(_.isLetterOrDigit)).mkString("_")
    s"test_$suffix".toLowerCase()
  }

  def conn: CassandraConnector = ???
  def pv = conn.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersion)

  def report(message:String): Unit = {}

  val ks = getKsName

  def skipIfProtocolVersionGTE(protocolVersion: ProtocolVersion)(f: => Unit): Unit = {
    if (!(pv.toInt >= protocolVersion.toInt)) f
      else report (s"Skipped Because ProtcolVersion $pv >= $protocolVersion")
  }

  def skipIfProtocolVersionLT(protocolVersion: ProtocolVersion)(f: => Unit): Unit = {
    if (!(pv.toInt < protocolVersion.toInt)) f
      else report (s"Skipped Because ProtocolVersion $pv < $protocolVersion")
  }

  implicit val ec = SparkCassandraITSpecBase.ec

  def awaitAll(units: Future[Unit]*): Unit = {
    implicit val ec = scala.concurrent.ExecutionContext.global
    Await.result(Future.sequence(units), Duration.Inf)
  }

  def keyspaceCql(name: String = ks) =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $name
       |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
       |AND durable_writes = false
       |""".stripMargin

  def createKeyspace(session: Session, name: String = ks): Unit = {
    session.execute(s"DROP KEYSPACE IF EXISTS $name")
    session.execute(keyspaceCql(name))
  }

  def restoreSystemProps(): Unit = {
    sys.props ++= originalProps
    sys.props --= (sys.props.keySet -- originalProps.keySet)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    clearCache()
    restoreSystemProps()
  }
}

object SparkCassandraITSpecBase {
  val executor = Executors.newFixedThreadPool(100)
  val ec = ExecutionContext.fromExecutor(executor)

  EmbeddedCassandra.removeShutdownHook
  // now embedded C* won't shutdown itself, let's do it in serial fashion
  SerialShutdownHooks.add("Shutting down all Cassandra runners")(() => {
    EmbeddedCassandra.shutdown
  })

}
