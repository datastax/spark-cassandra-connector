package com.datastax.spark.connector.bulk

import com.datastax.driver.core.DataType
import com.datastax.spark.connector.bulk.BulkConf._
import com.datastax.spark.connector.cql.{RegularColumn, ColumnDef}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.{ConfigCheck, ConfigParameter}
import com.datastax.spark.connector.writer.{PerRowWriteOptionValue, WriteOption, TimestampOption, TTLOption}
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption
import org.apache.cassandra.dht.{Murmur3Partitioner, RandomPartitioner, ByteOrderedPartitioner, IPartitioner}
import org.apache.spark.SparkConf

/**
 * Bulk loading settings.
 *
 * @param partitioner the 'partitioner' defined in cassandra.yaml.
 * @param serverConf the server side encryption configurations defined in cassandra.yaml.
 * @param throughputMiBPS the maximum throughput to throttle.
 * @param ttl the default TTL value which is used when it is defined (in seconds)
 * @param timestamp the default timestamp value which is used when it is defined (in microseconds)
 */
case class BulkConf
(
  partitioner: String = BulkConf.PartitionerParam.default,
  serverConf: BulkServerConf = BulkConf.DefaultBulkServerConf,
  throughputMiBPS: Int = BulkConf.ThroughputMiBPSParam.default,
  ttl: TTLOption = TTLOption.defaultValue,
  timestamp: TimestampOption = TimestampOption.defaultValue
) {
  require(BulkConf.AllowedPartitioners.contains(partitioner),
    s"Invalid value of spark.cassandra.bulk.partitioner: $partitioner. Expected any of ${BulkConf.AllowedPartitioners.mkString(", ")}."
  )

  private[bulk] def getPartitioner: IPartitioner = {
    partitioner match {
      case "org.apache.cassandra.dht.Murmur3Partitioner" => Murmur3Partitioner.instance
      case "org.apache.cassandra.dht.RandomPartitioner" => RandomPartitioner.instance
      case "org.apache.cassandra.dht.ByteOrderedPartitioner" => ByteOrderedPartitioner.instance
    }
  }

  private[bulk] val optionPlaceholders: Seq[String] = Seq(ttl, timestamp).collect {
    case WriteOption(PerRowWriteOptionValue(placeholder)) => placeholder
  }

  private[bulk] val optionsAsColumns: (String, String) => Seq[ColumnDef] = { (keyspace, table) =>
    def toRegularColDef(opt: WriteOption[_], dataType: DataType) = opt match {
      case WriteOption(PerRowWriteOptionValue(placeholder)) =>
        Some(ColumnDef(placeholder, RegularColumn, ColumnType.fromDriverType(dataType)))
      case _ => None
    }

    Seq(toRegularColDef(ttl, DataType.cint()), toRegularColDef(timestamp, DataType.bigint())).flatten
  }

  val throttlingEnabled = throughputMiBPS < BulkConf.ThroughputMiBPSParam.default
}

object BulkConf {

  /**
   * Bulk load server encryption settings.
   *
   * @param storagePort the 'storage_port' defined in cassandra.yaml.
   * @param sslStoragePort the 'ssl_storage_port' defined in cassandra.yaml.
   * @param internodeEncryption the 'server_encryption_options:internode_encryption' defined in cassandra.yaml.
   * @param keyStorePath the 'server_encryption_options:keystore' defined in cassandra.yaml.
   * @param keyStorePassword the 'server_encryption_options:keystore_password' defined in cassandra.yaml.
   * @param trustStorePath the 'server_encryption_options:truststore' defined in cassandra.yaml.
   * @param trustStorePassword the 'server_encryption_options:truststore_password' defined in cassandra.yaml.
   * @param protocol the 'server_encryption_options:protocol' defined in cassandra.yaml.
   * @param algorithm the 'server_encryption_options:algorithm' defined in cassandra.yaml.
   * @param storeType the 'server_encryption_options:store_type' defined in cassandra.yaml.
   * @param cipherSuites the 'server_encryption_options:cipher_suites' defined in cassandra.yaml.
   * @param requireClientAuth the 'server_encryption_options:require_client_auth' defined in cassandra.yaml.
   */
  case class BulkServerConf
  (
    storagePort: Int = BulkConf.StoragePortParam.default,
    sslStoragePort: Int = BulkConf.SSLStoragePortParam.default,
    internodeEncryption: String = BulkConf.InternodeEncryptionParam.default,
    keyStorePath: String = BulkConf.KeystorePathParam.default,
    keyStorePassword: String = BulkConf.KeystorePasswordParam.default,
    trustStorePath: String = BulkConf.TrustStorePathParam.default,
    trustStorePassword: String = BulkConf.TrustStorePasswordParam.default,
    protocol: String = BulkConf.ProtocolParam.default,
    algorithm: String = BulkConf.AlgorithmParam.default,
    storeType: String = BulkConf.StoreTypeParam.default,
    cipherSuites: Set[String] = BulkConf.CipherSuitesParam.default,
    requireClientAuth: Boolean = BulkConf.RequireClientAuthParam.default
  ) {
    require(BulkConf.AllowedInternodeEncryptions.contains(internodeEncryption),
      s"Invalid value of spark.cassandra.bulk.server.internode.encryption: ${internodeEncryption}. Expected any of ${BulkConf.AllowedInternodeEncryptions.mkString(", ")}."
    )

    private[bulk] def getServerEncryptionOptions: ServerEncryptionOptions = {
      val actualInternodeEncryption = internodeEncryption match {
        case "all" => InternodeEncryption.all
        case "none" => InternodeEncryption.none
        case "dc" => InternodeEncryption.dc
        case "rack" => InternodeEncryption.rack
      }

      val encryptionOptions = new ServerEncryptionOptions()
      encryptionOptions.internode_encryption = actualInternodeEncryption
      encryptionOptions.keystore = keyStorePath
      encryptionOptions.keystore_password = keyStorePassword
      encryptionOptions.truststore = trustStorePath
      encryptionOptions.truststore_password = trustStorePassword
      encryptionOptions.cipher_suites = cipherSuites.toArray
      encryptionOptions.protocol = protocol
      encryptionOptions.algorithm = algorithm
      encryptionOptions.store_type = storeType
      encryptionOptions.require_client_auth = requireClientAuth

      encryptionOptions
    }
  }

  val AllowedPartitioners = Set(
    "org.apache.cassandra.dht.Murmur3Partitioner",
    "org.apache.cassandra.dht.RandomPartitioner",
    "org.apache.cassandra.dht.ByteOrderedPartitioner"
  )

  val AllowedInternodeEncryptions = Set(
    "all",
    "none",
    "dc",
    "other"
  )

  val ReferenceSection = "Bulk Loading Parameters"

  val PartitionerParam = ConfigParameter[String](
    name = "spark.cassandra.bulk.partitioner",
    section = ReferenceSection,
    default = "org.apache.cassandra.dht.Murmur3Partitioner",
    description = """The 'partitioner' defined in cassandra.yaml."""
  )

  val ThroughputMiBPSParam = ConfigParameter[Int] (
    name = "spark.cassandra.bulk.throughput_mb_per_sec",
    section = ReferenceSection,
    default = Int.MaxValue,
    description = """Maximum write throughput allowed per single core in MB/s.""".stripMargin
  )

  val StoragePortParam = ConfigParameter[Int](
    name = "spark.cassandra.bulk.server.storage.port",
    section = ReferenceSection,
    default = 7000,
    description = """The 'storage_port' defined in cassandra.yaml."""
  )

  val SSLStoragePortParam = ConfigParameter[Int](
    name = "spark.cassandra.bulk.server.sslStorage.port",
    section = ReferenceSection,
    default = 7001,
    description = """The 'ssl_storage_port' defined in cassandra.yaml."""
  )

  val InternodeEncryptionParam = ConfigParameter[String](
    name = "spark.cassandra.bulk.server.internode.encryption",
    section = ReferenceSection,
    default = "none",
    description = """The 'server_encryption_options:internode_encryption' defined in cassandra.yaml."""
  )

  val KeystorePathParam = ConfigParameter[String](
    name = "spark.cassandra.bulk.server.keyStore.path",
    section = ReferenceSection,
    default = "conf/.keystore",
    description = """The 'server_encryption_options:keystore' defined in cassandra.yaml."""
  )

  val KeystorePasswordParam = ConfigParameter[String](
    name = "spark.cassandra.bulk.server.keyStore.password",
    section = ReferenceSection,
    default = "cassandra",
    description = """The 'server_encryption_options:keystore_password' defined in cassandra.yaml."""
  )

  val TrustStorePathParam = ConfigParameter[String](
    name = "spark.cassandra.bulk.server.trustStore.path",
    section = ReferenceSection,
    default = "conf/.truststore",
    description = """The 'server_encryption_options:truststore' defined in cassandra.yaml."""
  )

  val TrustStorePasswordParam = ConfigParameter[String](
    name = "spark.cassandra.bulk.server.trustStore.password",
    section = ReferenceSection,
    default = "cassandra",
    description = """The 'server_encryption_options:truststore_password' defined in cassandra.yaml."""
  )

  val ProtocolParam = ConfigParameter[String](
    name = "spark.cassandra.bulk.server.protocol",
    section = ReferenceSection,
    default = "TLS",
    description = """The 'server_encryption_options:protocol' defined in cassandra.yaml."""
  )

  val AlgorithmParam = ConfigParameter[String](
    name = "spark.cassandra.bulk.server.algorithm",
    section = ReferenceSection,
    default = "SunX509",
    description = """The 'server_encryption_options:algorithm' defined in cassandra.yaml."""
  )

  val StoreTypeParam = ConfigParameter[String](
    name = "spark.cassandra.bulk.server.store.type",
    section = ReferenceSection,
    default = "JKS",
    description = """The 'server_encryption_options:store_type' defined in cassandra.yaml."""
  )

  val CipherSuitesParam = ConfigParameter[Set[String]](
    name = "spark.cassandra.bulk.server.cipherSuites",
    section = ReferenceSection,
    default = Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"),
    description = """The 'server_encryption_options:cipher_suites' defined in cassandra.yaml."""
  )

  val RequireClientAuthParam = ConfigParameter[Boolean](
    name = "spark.cassandra.bulk.server.requireClientAuth",
    section = ReferenceSection,
    default = false,
    description = """The 'server_encryption_options:require_client_auth' defined in cassandra.yaml."""
  )

  val Properties: Set[ConfigParameter[_]] = Set(
    PartitionerParam,
    ThroughputMiBPSParam,
    StoragePortParam,
    SSLStoragePortParam,
    InternodeEncryptionParam,
    KeystorePathParam,
    KeystorePasswordParam,
    TrustStorePathParam,
    TrustStorePasswordParam,
    ProtocolParam,
    AlgorithmParam,
    StoreTypeParam,
    CipherSuitesParam,
    RequireClientAuthParam
  )

  val DefaultBulkServerConf = BulkServerConf()

  def fromSparkConf(conf: SparkConf): BulkConf = {
    ConfigCheck.checkConfig(conf)

    val partitioner = conf.get(PartitionerParam.name, PartitionerParam.default)
    val throughputMiBPS = conf.getInt(ThroughputMiBPSParam.name, ThroughputMiBPSParam.default)
    val storagePort = conf.getInt(StoragePortParam.name, StoragePortParam.default)
    val sslStoragePort = conf.getInt(SSLStoragePortParam.name, SSLStoragePortParam.default)
    val internodeEncryption = conf.get(InternodeEncryptionParam.name, InternodeEncryptionParam.default)
    val keyStorePath = conf.get(KeystorePathParam.name, KeystorePathParam.default)
    val keyStorePassword = conf.get(KeystorePasswordParam.name, KeystorePasswordParam.default)
    val trustStorePath = conf.get(TrustStorePathParam.name, TrustStorePathParam.default)
    val trustStorePassword = conf.get(TrustStorePasswordParam.name, TrustStorePasswordParam.default)
    val protocol = conf.get(ProtocolParam.name, ProtocolParam.default)
    val algorithm = conf.get(AlgorithmParam.name, AlgorithmParam.default)
    val storeType = conf.get(StoreTypeParam.name, StoreTypeParam.default)
    val cipherSuites = conf.getOption(CipherSuitesParam.name)
      .map(_.split(",").map(_.trim).toSet).getOrElse(CipherSuitesParam.default)
    val requireClientAuth = conf.getBoolean(RequireClientAuthParam.name, RequireClientAuthParam.default)

    val bulkServerConf = BulkServerConf(
      storagePort = storagePort,
      sslStoragePort = sslStoragePort,
      internodeEncryption = internodeEncryption,
      keyStorePath = keyStorePath,
      keyStorePassword = keyStorePassword,
      trustStorePath = trustStorePath,
      trustStorePassword = trustStorePassword,
      protocol = protocol,
      algorithm = algorithm,
      storeType = storeType,
      cipherSuites = cipherSuites,
      requireClientAuth = requireClientAuth
    )

    BulkConf(
      partitioner = partitioner,
      serverConf = bulkServerConf,
      throughputMiBPS = throughputMiBPS
    )
  }
}