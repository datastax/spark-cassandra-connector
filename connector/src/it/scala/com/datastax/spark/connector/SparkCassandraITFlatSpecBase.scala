package com.datastax.spark.connector

import java.util.concurrent.Executors

import com.datastax.bdp.hadoop.hive.metastore.CassandraClientConfiguration

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import org.apache.commons.lang3.StringUtils
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}
import com.datastax.driver.core.{CCMBridge, ProtocolVersion, Session}
import com.datastax.spark.connector.cql.{CassandraConnector, DefaultAuthConfFactory}
import com.datastax.spark.connector.cql.CassandraConnectorConf._
import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.testkit.AbstractSpec
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkCassandraITFlatSpecBase extends FlatSpec with SparkCassandraITSpecBase {
  override def report(message: String): Unit = info
}

trait SparkCassandraITWordSpecBase extends WordSpec with SparkCassandraITSpecBase

trait SparkCassandraITAbstractSpecBase extends AbstractSpec with SparkCassandraITSpecBase

object CCMTraits {

  sealed trait CCMTrait
  case class Auth() extends CCMTrait
  case class SSL() extends CCMTrait

}

trait SparkCassandraITSpecBase
  extends Suite
  with Matchers
  with BeforeAndAfterAll {

  import CCMTraits._

  sys.env.get("DSE_HOME").foreach { home =>
    System.setProperty("dse", "true")
    System.setProperty("cassandra.directory", home)
    System.setProperty("cassandra.version", "6.8")
    System.setProperty("cassandra.branch", "master")
  }

  final val ccmBridgeBuilder: CCMBridge.Builder = CCMBridge.builder()

  lazy val traits: Set[CCMTrait] = Set.empty

  def applyTraits(b: CCMBridge.Builder): CCMBridge.Builder = {
    traits.foldRight(b) {
      case (auth: Auth, builder) => builder.withAuth()
      case (ssl: SSL, builder) => builder.withSSL()
    }
  }

  lazy val ccmBridge = synchronized {
    SparkCassandraITSpecBase.ccmBridge match {
      case None =>
        val newBridge = applyTraits(ccmBridgeBuilder).build()
        SparkCassandraITSpecBase.ccmBridge = Some(newBridge)
        newBridge.start()
        newBridge
      case Some(bridge) =>
        bridge
    }
  }

  def getConnectionHost = ccmBridge.addressOfNode(1).getHostName
  def getConnectionPort = ccmBridge.addressOfNode(1).getPort.toString


  def connectionParameters = {
    val basic = Map(
      ConnectionHostParam.name -> getConnectionHost,
      ConnectionPortParam.name -> getConnectionPort,
      s"spark.hadoop.${CassandraClientConfiguration.CONF_PARAM_HOST}" -> getConnectionHost,
      s"spark.hadoop.${CassandraClientConfiguration.CONF_PARAM_NATIVE_PORT}" -> getConnectionPort
    )

    val SslParams = Seq(
        SSLEnabledParam.name -> "true",
        SSLClientAuthEnabledParam.name -> "true",
        SSLTrustStorePasswordParam.name -> CCMBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD,
        SSLTrustStorePathParam.name -> CCMBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getPath,
        SSLKeyStorePasswordParam.name -> CCMBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD,
        SSLKeyStorePathParam.name -> CCMBridge.DEFAULT_CLIENT_KEYSTORE_FILE.getPath
      )

    val AuthParams = Seq(
        DefaultAuthConfFactory.UserNameParam.name -> "cassandra",
        DefaultAuthConfFactory.PasswordParam.name -> "cassandra"
      )

    val params = traits.foldRight(basic) {
      case (auth: Auth, options) => options ++ AuthParams ++ SslParams
      case (ssL: SSL, options) => options ++  SslParams
    }
    println(params)

    params
  }


  final def defaultConf: SparkConf = {
    SparkTemplate.defaultConf
      .setAll(connectionParameters)
  }
  final def sparkConf = defaultConf

  lazy val spark = SparkSession.builder().config(sparkConf).getOrCreate().newSession()
  lazy val sparkSession = spark
  lazy val sc = spark.sparkContext

  val originalProps = sys.props.clone()

  final override def beforeAll(): Unit = {
    ccmBridge.waitForUp(1) // Init Bridge
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

  def pv = conn.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersion)

  def report(message: String): Unit = {}

  val ks = getKsName

  def skipIfProtocolVersionGTE(protocolVersion: ProtocolVersion)(f: => Unit): Unit = {
    if (!(pv.toInt >= protocolVersion.toInt)) f
    else report(s"Skipped Because ProtcolVersion $pv >= $protocolVersion")
  }

  def skipIfProtocolVersionLT(protocolVersion: ProtocolVersion)(f: => Unit): Unit = {
    if (!(pv.toInt < protocolVersion.toInt)) f
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

  def createKeyspace(session: Session, name: String = ks): Unit = {
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
          .getCluster
          .getMetadata
          .getKeyspace(ks)
          .getTables()
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

  var ccmBridge: Option[CCMBridge] = None
}
