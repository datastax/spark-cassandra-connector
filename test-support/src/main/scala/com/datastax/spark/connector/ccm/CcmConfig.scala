package com.datastax.spark.connector.ccm

import java.io.{File, IOException, InputStream}
import java.net.InetSocketAddress
import java.nio.file.{Files, StandardCopyOption}

import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.ccm.CcmConfig._
import org.slf4j.{Logger, LoggerFactory}

case class CcmConfig(
    nodes: Seq[Int] = Seq(1),
    // disable auto_snapshot by default to reduce disk usage when destroying schema.
    cassandraConfiguration: Map[String, Object] = Map("auto_snapshot" -> "false"),
    dseConfiguration: Map[String, Object] = Map(),
    dseRawYaml: Seq[String] = List(),
    jvmArgs: Seq[String] = List(),
    ipPrefix: String = "127.0.0.",
    createOptions: List[String] = List(),
    dseWorkloads: List[String] = List(),
    jmxPortOffset: Int = 0,
    version: Version = Version.parse(System.getProperty("ccm.version", "3.11.0")),
    installDirectory: Option[String] = Option(System.getProperty("ccm.directory")),
    installBranch: Option[String] = Option(System.getProperty("ccm.branch")),
    dseEnabled: Boolean = Option(System.getProperty("ccm.dse")).exists(_.toBoolean),
    mode: ClusterMode = ClusterMode.Standard) {

  def withSsl(): CcmConfig = {
    copy(cassandraConfiguration = cassandraConfiguration +
      ("client_encryption_options.enabled" -> "true") +
      ("client_encryption_options.keystore" -> DEFAULT_SERVER_KEYSTORE_FILE.getAbsolutePath) +
      ("client_encryption_options.keystore_password" -> DEFAULT_SERVER_KEYSTORE_PASSWORD)
    )
  }

  def withSslLocalhostCn(): CcmConfig = {
    copy(cassandraConfiguration = cassandraConfiguration +
      ("client_encryption_options.enabled" -> "true") +
      ("client_encryption_options.keystore" -> DEFAULT_SERVER_LOCALHOST_KEYSTORE_FILE.getAbsolutePath) +
      ("client_encryption_options.keystore_password" -> DEFAULT_SERVER_KEYSTORE_PASSWORD))
  }

  /** Enables client authentication. This also enables encryption ({@link #withSsl()}. */
  def withSslAuth(): CcmConfig = {
    withSsl().copy(cassandraConfiguration = cassandraConfiguration +
      ("client_encryption_options.require_client_auth" -> "true") +
      ("client_encryption_options.truststore" -> DEFAULT_SERVER_TRUSTSTORE_FILE.getAbsolutePath) +
      ("client_encryption_options.truststore_password" -> DEFAULT_SERVER_TRUSTSTORE_PASSWORD)
    )
  }

  def getDseVersion: Option[Version] = {
    if (dseEnabled) Option(version) else None
  }

  def getCassandraVersion: Version = {
    if (!dseEnabled) {
      version
    } else {
      val stableVersion = version.nextStable()
      if (stableVersion.compareTo(V6_0_0) >= 0) {
        V4_0_0
      } else if (stableVersion.compareTo(V5_1_0) >= 0) {
        V3_10
      } else if (stableVersion.compareTo(V5_0_0) >= 0) {
        V3_0_15
      } else {
        V2_1_19
      }
    }
  }

  def ipOfNode(n: Int): String = {
    ipPrefix + n
  }

  def jmxPort(n: Int): Integer = {
    7108 + jmxPortOffset + n
  }

  def addressOfNode(n: Int): InetSocketAddress = {
    new InetSocketAddress(ipOfNode(n), 9042)
  }

  def nodeAddresses(): Seq[InetSocketAddress] = {
    nodes.map(addressOfNode)
  }
}

object CcmConfig {

  val logger: Logger = LoggerFactory.getLogger(classOf[CcmConfig])

  val DEFAULT_CLIENT_TRUSTSTORE_PASSWORD: String = "cassandra1sfun"
  val DEFAULT_CLIENT_TRUSTSTORE_PATH: String = "/client.truststore"

  val DEFAULT_CLIENT_TRUSTSTORE_FILE: File =
    createTempStore(DEFAULT_CLIENT_TRUSTSTORE_PATH)

  val DEFAULT_CLIENT_KEYSTORE_PASSWORD: String = "cassandra1sfun"
  val DEFAULT_CLIENT_KEYSTORE_PATH: String = "/client.keystore"

  val DEFAULT_CLIENT_KEYSTORE_FILE: File =
    createTempStore(DEFAULT_CLIENT_KEYSTORE_PATH)

  // Contains the same keypair as the client keystore, but in format usable by OpenSSL
  val DEFAULT_CLIENT_PRIVATE_KEY_FILE: File = createTempStore("/client.key")
  val DEFAULT_CLIENT_CERT_CHAIN_FILE: File = createTempStore("/client.crt")

  val DEFAULT_SERVER_TRUSTSTORE_PASSWORD: String = "cassandra1sfun"
  val DEFAULT_SERVER_TRUSTSTORE_PATH: String = "/server.truststore"

  val DEFAULT_SERVER_TRUSTSTORE_FILE: File =
    createTempStore(DEFAULT_SERVER_TRUSTSTORE_PATH)

  val DEFAULT_SERVER_KEYSTORE_PASSWORD: String = "cassandra1sfun"
  val DEFAULT_SERVER_KEYSTORE_PATH: String = "/server.keystore"

  val DEFAULT_SERVER_KEYSTORE_FILE: File =
    createTempStore(DEFAULT_SERVER_KEYSTORE_PATH)

  // A separate keystore where the certificate has a CN of localhost, used for hostname
  // validation testing.
  val DEFAULT_SERVER_LOCALHOST_KEYSTORE_PATH: String = "/server_localhost.keystore"

  val DEFAULT_SERVER_LOCALHOST_KEYSTORE_FILE: File =
    createTempStore(DEFAULT_SERVER_LOCALHOST_KEYSTORE_PATH)

  // major DSE versions
  val V6_0_0: Version = Version.parse("6.0.0")
  val V5_1_0: Version = Version.parse("5.1.0")
  val V5_0_0: Version = Version.parse("5.0.0")

  // mapped C* versions from DSE versions
  val V4_0_0: Version = Version.parse("4.0.0")
  val V3_10: Version = Version.parse("3.10")
  val V3_0_15: Version = Version.parse("3.0.15")
  val V2_1_19: Version = Version.parse("2.1.19")

  // artificial estimation of maximum number of nodes for this cluster, may be bumped anytime.
  val MAX_NUMBER_OF_NODES: Integer = 4

  /**
    * Extracts a keystore from the classpath into a temporary file.
    *
    * <p>This is needed as the keystore could be part of a built test jar used by other projects, and
    * they need to be extracted to a file system so cassandra may use them.
    *
    * @param storePath Path in classpath where the keystore exists.
    * @return The generated File.
    */
  private def createTempStore(storePath: String): File = {
    var file: File = null
    try {
      file = File.createTempFile("server", ".store")
      file.deleteOnExit()
      val resource: InputStream = this.getClass.getResourceAsStream(storePath)
      if (resource != null) {
        Files.copy(resource, file.toPath, StandardCopyOption.REPLACE_EXISTING)
      } else {
        throw new IllegalStateException("Store path not found: " + storePath)
      }
    } catch {
      case e: IOException =>
        logger.warn("Failure to write keystore, SSL-enabled servers may fail to start.", e)
    }
    file
  }

}