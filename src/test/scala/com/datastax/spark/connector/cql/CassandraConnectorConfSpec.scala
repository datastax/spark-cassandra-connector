package com.datastax.spark.connector.cql

import java.net.InetAddress

import scala.language.postfixOps

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class CassandraConnectorConfSpec extends FlatSpec with Matchers {

  it should "be serializable" in {
    val conf = CassandraConnectorConf(new SparkConf)
    SerializationUtils.roundtrip(conf)
  }

  it should "match a conf with the same settings" in {
    val conf_a = CassandraConnectorConf(new SparkConf)
    val conf_1 = CassandraConnectorConf(new SparkConf)

    conf_a should equal (conf_1)
  }

  it should "match a conf with all the same settings but maxConnectionsPerHost" in {
    val conf_a = CassandraConnectorConf(
      new SparkConf().set(CassandraConnectorConf.MaxRemoteConnectionsPerExecutorParam.name, "20"))
    val conf_b = CassandraConnectorConf(
      new SparkConf().set(CassandraConnectorConf.MaxRemoteConnectionsPerExecutorParam.name, "13"))

    conf_a should equal (conf_b)
  }

  it should "resolve default SSL settings correctly" in {
    val sparkConf = new SparkConf(loadDefaults = false)

    val connConf = CassandraConnectorConf(sparkConf)
    connConf.cassandraSSLConf.enabled shouldBe false
    connConf.cassandraSSLConf.trustStorePath shouldBe empty
    connConf.cassandraSSLConf.trustStorePassword shouldBe empty
    connConf.cassandraSSLConf.trustStoreType shouldBe "JKS"
    connConf.cassandraSSLConf.protocol shouldBe "TLS"
    connConf.cassandraSSLConf.enabledAlgorithms should contain theSameElementsAs Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
    connConf.cassandraSSLConf.clientAuthEnabled shouldBe false
    connConf.cassandraSSLConf.keyStorePath shouldBe empty
    connConf.cassandraSSLConf.keyStorePassword shouldBe empty
    connConf.cassandraSSLConf.keyStoreType shouldBe "JKS"
  }

  it should "resolve provided SSL settings correctly" in {
    val sparkConf = new SparkConf(loadDefaults = false)
    sparkConf.set(CassandraConnectorConf.SSLEnabledParam.name, "true")
    sparkConf.set(CassandraConnectorConf.SSLTrustStorePathParam.name, "/etc/keys/.truststore")
    sparkConf.set(CassandraConnectorConf.SSLTrustStorePasswordParam.name, "secret")
    sparkConf.set(CassandraConnectorConf.SSLTrustStoreTypeParam.name, "JCEKS")
    sparkConf.set(CassandraConnectorConf.SSLProtocolParam.name, "SSLv3")
    sparkConf.set(CassandraConnectorConf.SSLEnabledAlgorithmsParam.name, "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384,TLS_DHE_RSA_WITH_AES_256_CBC_SHA256")
    sparkConf.set(CassandraConnectorConf.SSLClientAuthEnabledParam.name, "true")
    sparkConf.set(CassandraConnectorConf.SSLKeyStorePathParam.name, "/etc/keys/.keystore")
    sparkConf.set(CassandraConnectorConf.SSLKeyStorePasswordParam.name, "secret")
    sparkConf.set(CassandraConnectorConf.SSLKeyStoreTypeParam.name,  "JCEKS")

    val connConf = CassandraConnectorConf(sparkConf)
    connConf.cassandraSSLConf.enabled shouldBe true
    connConf.cassandraSSLConf.trustStorePath shouldBe Some("/etc/keys/.truststore")
    connConf.cassandraSSLConf.trustStorePassword shouldBe Some("secret")
    connConf.cassandraSSLConf.trustStoreType shouldBe "JCEKS"
    connConf.cassandraSSLConf.protocol shouldBe "SSLv3"
    connConf.cassandraSSLConf.enabledAlgorithms should contain theSameElementsAs Seq("TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256")
    connConf.cassandraSSLConf.clientAuthEnabled shouldBe true
    connConf.cassandraSSLConf.keyStorePath shouldBe Some("/etc/keys/.keystore")
    connConf.cassandraSSLConf.keyStorePassword shouldBe Some("secret")
    connConf.cassandraSSLConf.keyStoreType shouldBe "JCEKS"
  }

  it should "produce serializedConfString that disregards names in hosts" in {
    val addressAndHost = InetAddress.getLocalHost
    val addressOnly = InetAddress.getByAddress(addressAndHost.getAddress)

    val addressOnlyConf = CassandraConnectorConf(hosts = Set(addressOnly))
    val addressAndHostConf = CassandraConnectorConf(hosts = Set(addressAndHost))

    addressOnlyConf.serializedConfString should be (addressAndHostConf.serializedConfString)
  }

}
