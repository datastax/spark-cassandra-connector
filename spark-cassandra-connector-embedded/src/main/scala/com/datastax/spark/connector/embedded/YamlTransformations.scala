package com.datastax.spark.connector.embedded

import java.lang.{Boolean => JBoolean, Integer => JInteger}
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption.{CREATE, TRUNCATE_EXISTING}
import java.nio.file.{Files, Path, Paths}
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.yaml.snakeyaml.{DumperOptions, Yaml}

class YamlTransformations {

  private val transformations = ArrayBuffer[JMap[String, AnyRef] => Unit]()

  def getTransformations: List[JMap[String, AnyRef] => Unit] = transformations.toList

  def addTransformation(replacement: JMap[String, AnyRef] => Unit): YamlTransformations = {
    transformations += replacement
    this
  }

  def addTransformation(name: String, value: AnyRef): YamlTransformations = {
    transformations += {
      _.put(name, value)
    }
    this
  }

  def and(other: YamlTransformations): YamlTransformations = {
    val result = new YamlTransformations
    result.transformations ++= this.getTransformations
    result.transformations ++= other.getTransformations
    result
  }
}

object YamlTransformations {
  lazy val YamlTemplateVersion = {
    import EmbeddedCassandra._
    s"$cassandraMajorVersion.$cassandraMinorVersion"
  }

  private val yaml = new Yaml()

  private def readTemplate(): JMap[String, AnyRef]@unchecked = {
    val exactTmplt =
      Option(ClassLoader.getSystemResourceAsStream(s"cassandra-$YamlTemplateVersion.yaml.template"))
    lazy val fallbackTmplt = {
      System.err.println(s"Warning: Using fallback template for Cassandra 3.2 because " +
        s"the template for Cassandra $YamlTemplateVersion could not be found.")
      Option(ClassLoader.getSystemResourceAsStream(s"cassandra-3.2.yaml.template"))
    }

    yaml.load(exactTmplt orElse fallbackTmplt orNull) match {
      case map: JMap[String, AnyRef]@unchecked => map
    }
  }

  private def processTemplate(yamlTransformers: YamlTransformations*): String = {
    val template = readTemplate()
    for (replacement <- yamlTransformers.flatMap(_.transformations)) {
      replacement(template)
    }
    yaml.dumpAs(template, null, DumperOptions.FlowStyle.BLOCK)
  }

  def makeYaml(outputPath: Path, yamlTransformers: YamlTransformations*): Unit = {
    val output = processTemplate(yamlTransformers: _*)
    Files.write(outputPath, output.getBytes(StandardCharsets.UTF_8), CREATE, TRUNCATE_EXISTING)
  }

  object Default extends YamlTransformations {
    if (YamlTemplateVersion >= "2.2") {
      addTransformation("commitlog_compression", List(Map(
        "class_name" -> "LZ4Compressor"): JMap[String, AnyRef]): JList[AnyRef])
    }
  }

  object ClientEncryption extends YamlTransformations {
    addTransformation {
      _.get("client_encryption_options") match {
        case clientEncryptionOptions: JMap[String, AnyRef]@unchecked =>
          clientEncryptionOptions.put("enabled", JBoolean.TRUE)
          clientEncryptionOptions.put("cipher_suites", List("TLS_RSA_WITH_AES_128_CBC_SHA"): JList[String])
          clientEncryptionOptions.put("keystore", ClassLoader.getSystemResource("keystore").getPath)
          clientEncryptionOptions.put("keystore_password", "connector")
      }
    }
  }

  object ClientAuth extends YamlTransformations {
    addTransformation {
      _.get("client_encryption_options") match {
        case clientEncryptionOptions: JMap[String, AnyRef]@unchecked =>
          clientEncryptionOptions.put("require_client_auth", JBoolean.TRUE)
          clientEncryptionOptions.put("truststore", ClassLoader.getSystemResource("truststore").getPath)
          clientEncryptionOptions.put("truststore_password", "connector")
      }
    }
  }

  object PasswordAuth extends YamlTransformations {
    addTransformation(_.put("authenticator", "PasswordAuthenticator"))
  }

  case class CassandraConfiguration(
     clusterName: String = "Test Cluster",
     cassandraDir: String = "./data",
     seeds: List[String] = List("127.0.0.1"),
     storagePort: Int = 7000,
     sslStoragePort: Int = 7001,
     listenAddress: String = "127.0.0.1",
     nativeTransportPort: Int = 9042,
     rpcAddress: String = "127.0.0.1",
     jmxPort: Int = CassandraRunner.DefaultJmxPort) extends YamlTransformations {

    addTransformation("cluster_name", clusterName)
    addTransformation("data_file_directories", List(Paths.get(cassandraDir, "data").toString): JList[String])
    addTransformation("commitlog_directory", Paths.get(cassandraDir, "commitlog").toString)
    addTransformation("saved_caches_directory", Paths.get(cassandraDir, "saved_caches").toString)
    addTransformation("seed_provider", List(Map(
      "class_name" -> "org.apache.cassandra.locator.SimpleSeedProvider",
      "parameters" -> (List(Map("seeds" -> seeds.mkString(",")): JMap[String, String]): JList[JMap[String, String]])
    ): JMap[String, AnyRef]): JList[AnyRef])
    addTransformation("storage_port", storagePort: JInteger)
    addTransformation("ssl_storage_port", sslStoragePort: JInteger)
    addTransformation("listen_address", listenAddress)
    addTransformation("native_transport_port", nativeTransportPort: JInteger)
    addTransformation("rpc_address", rpcAddress)

    if (YamlTemplateVersion >= "3.0") {
      addTransformation("hints_directory", Paths.get(cassandraDir, "hints").toString)
    }
  }

}
