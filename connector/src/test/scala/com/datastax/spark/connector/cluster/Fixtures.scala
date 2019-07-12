package com.datastax.spark.connector.cluster

import com.datastax.bdp.hadoop.hive.metastore.CassandraClientConfiguration
import com.datastax.spark.connector.ccm.CcmBridge
import com.datastax.spark.connector.cql.CassandraConnectorConf._
import com.datastax.spark.connector.cql.DefaultAuthConfFactory
import org.slf4j.MDC

/** Defines connection properties for a cluster defined by one of the sub-traits. In case of multi-node setup, this
  * defines connection properties for the first node. */
trait ConnectionProvider {

  def testCluster: TestCluster

  def testCluster(c: Int): TestCluster = if (c == 0) testCluster else
    throw new IllegalArgumentException(s"This provider defines only one cluster, cluster $c does not exist")

  def connectionParameters: Map[String, String]

  def connectionParameters(c: Int): Map[String, String] = if (c == 0) connectionParameters else
    throw new IllegalArgumentException(s"This provider defines only one cluster, cluster $c does not exist")
}

/** Encapsulates cluster configuration. Sub-traits define a builder that configures a cluster.
  *
  * Integration tests that share the same fixture, share the same process during execution.
  *
  * Callers are purposefully denied an ability to configure clusters on spot. Creating many different configurations
  * would result in additional time wasted for bootstrapping clusters (as every configuration needs cluster bootstrap
  * and eventually teardown). Desired usage pattern is utilizing one of the following predefined fixtures.
  *
  * For the same reason, please think twice before adding another [[Fixture]] sub-trait. */
sealed trait Fixture extends ConnectionProvider {

  protected[cluster] def groupNumber: Int = sys.env.getOrElse("TEST_GROUP_NO", "0").toInt
  protected[cluster] def ipPrefix: String = sys.env.getOrElse("CCM_IP_PREFIX", "127.0.")

  /* Integration tests logs are prefixed with [Tx] to denote which test group/process produced the given log line */
  MDC.put("TEST_GROUP_NO", groupNumber.toString)

  sys.env.get("DSE_HOME").foreach { home =>
    System.setProperty("dse", "true")
    System.setProperty("ccm.directory", home)
    System.setProperty("ccm.version", "6.8")
    System.setProperty("ccm.branch", "master")
  }

  private[cluster] def builders: Seq[CcmBridge.Builder]

  protected val defaultBuilder: CcmBridge.Builder = CcmBridge.builder()
    .withIpPrefix(ipPrefix + "0.")
    .withJMXPortOffset(CcmBridge.MAX_NUMBER_OF_NODES * groupNumber)
}

sealed trait SingleClusterFixture extends Fixture {
  override def testCluster: TestCluster = ClusterHolder.get(this).head
}

/** Most of the integration tests use this cluster configuration. It has no auth configured. Reuse this cluster config
  * whenever is is possible. */
trait DefaultCluster extends SingleClusterFixture {

  private[cluster] final override val builders: Seq[CcmBridge.Builder] = Seq(defaultBuilder)

  override def connectionParameters: Map[String, String] =
    DefaultCluster.defaultConnectionParameters(this.testCluster)
}

object DefaultCluster {
  def defaultConnectionParameters(cluster: TestCluster): Map[String, String] = {
    Map(
      ConnectionHostParam.name -> cluster.getConnectionHost,
      ConnectionPortParam.name -> cluster.getConnectionPort,
      s"spark.hadoop.${CassandraClientConfiguration.CONF_PARAM_HOST}" -> cluster.getConnectionHost,
      s"spark.hadoop.${CassandraClientConfiguration.CONF_PARAM_NATIVE_PORT}" -> cluster.getConnectionPort
    )
  }
}

/** SSL enabled cluster configuration. It has no auth configured. */
trait SSLCluster extends SingleClusterFixture {

  private[cluster] final override val builders: Seq[CcmBridge.Builder] = Seq(defaultBuilder.withSsl())

  override def connectionParameters: Map[String, String] =
    DefaultCluster.defaultConnectionParameters(this.testCluster) ++
    SSLCluster.defaultConnectionParameters()
}

object SSLCluster {
  def defaultConnectionParameters(): Map[String, String] = {
    Map(
      SSLEnabledParam.name -> "true",
      SSLClientAuthEnabledParam.name -> "true",
      SSLTrustStorePasswordParam.name -> CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD,
      SSLTrustStorePathParam.name -> CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getPath,
      SSLKeyStorePasswordParam.name -> CcmBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD,
      SSLKeyStorePathParam.name -> CcmBridge.DEFAULT_CLIENT_KEYSTORE_FILE.getPath
    )
  }
}

/** SSL enabled cluster configuration with auth configured. */
trait AuthCluster extends SingleClusterFixture {

  private[cluster] final override val builders: Seq[CcmBridge.Builder] = Seq(defaultBuilder.withSslAuth())

  override def connectionParameters: Map[String, String] =
    DefaultCluster.defaultConnectionParameters(this.testCluster) ++
    SSLCluster.defaultConnectionParameters() ++
    AuthCluster.defaultConnectionParameters()
}

object AuthCluster {
  def defaultConnectionParameters(): Map[String, String] = {
    Map(
      DefaultAuthConfFactory.UserNameParam.name -> "cassandra",
      DefaultAuthConfFactory.PasswordParam.name -> "cassandra"
    )
  }
}

/** A fixture that bootstraps two separate clusters with one node each. */
trait TwoClustersWithOneNode extends Fixture {

  private[cluster] final override val builders: Seq[CcmBridge.Builder] = for (i <- 0 to 1) yield
    CcmBridge.builder()
      .withIpPrefix(s"$ipPrefix$i.")
      .withJMXPortOffset(CcmBridge.MAX_NUMBER_OF_NODES * groupNumber + i)

  override def connectionParameters: Map[String, String] = connectionParameters(0)

  override def connectionParameters(c: Int): Map[String, String] =
    DefaultCluster.defaultConnectionParameters(testCluster(c))

  override def testCluster: TestCluster = testCluster(0)

  override def testCluster(c: Int): TestCluster = ClusterHolder.get(this)(c)
}

trait CETCluster extends DefaultCluster

trait PSTCluster extends DefaultCluster

/** Fixture marker that instructs test framework to execute the marked test within a separated process/JVM. */
trait SeparateJVM extends Fixture