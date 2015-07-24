package com.datastax.spark.connector.cql

import scala.language.postfixOps

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.SparkConf
import scala.concurrent.duration._
import org.scalatest.{Matchers, FlatSpec}

import com.datastax.spark.connector.cql.CassandraConnectorConf.RetryDelayConf

class CassandraConnectorConfSpec extends FlatSpec with Matchers {

  it should "be serializable" in {
    val conf = CassandraConnectorConf(new SparkConf)
    SerializationUtils.roundtrip(conf)
  }

  it should "resolve default SSL settings correctly" in {
    val sparkConf = new SparkConf(loadDefaults = false)

    val connConf = CassandraConnectorConf(sparkConf)
    connConf.cassandraSSLConf.enabled shouldBe false
    connConf.cassandraSSLConf.trustStorePath shouldBe empty
    connConf.cassandraSSLConf.trustStorePassword shouldBe empty
    connConf.cassandraSSLConf.trustStoreType shouldBe "JKS"
    connConf.cassandraSSLConf.protocol shouldBe "TLS"
    connConf.cassandraSSLConf.enabledAlgorithms should contain theSameElementsAs Seq("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
  }

  it should "resolve provided SSL settings correctly" in {
    val sparkConf = new SparkConf(loadDefaults = false)
    sparkConf.set("spark.cassandra.connection.ssl.enabled", "true")
    sparkConf.set("spark.cassandra.connection.ssl.trustStore.path", "/etc/keys/.truststore")
    sparkConf.set("spark.cassandra.connection.ssl.trustStore.password", "secret")
    sparkConf.set("spark.cassandra.connection.ssl.trustStore.type", "JCEKS")
    sparkConf.set("spark.cassandra.connection.ssl.protocol", "SSLv3")
    sparkConf.set("spark.cassandra.connection.ssl.enabledAlgorithms", "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384,TLS_DHE_RSA_WITH_AES_256_CBC_SHA256")

    val connConf = CassandraConnectorConf(sparkConf)
    connConf.cassandraSSLConf.enabled shouldBe true
    connConf.cassandraSSLConf.trustStorePath shouldBe Some("/etc/keys/.truststore")
    connConf.cassandraSSLConf.trustStorePassword shouldBe Some("secret")
    connConf.cassandraSSLConf.trustStoreType shouldBe "JCEKS"
    connConf.cassandraSSLConf.protocol shouldBe "SSLv3"
    connConf.cassandraSSLConf.enabledAlgorithms should contain theSameElementsAs Seq("TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256")
  }

  it should "resolve default retry delay settings correctly" in {
    val sparkConf = new SparkConf(loadDefaults = false)

    val connConf = CassandraConnectorConf(sparkConf)
    connConf.queryRetryDelay shouldBe CassandraConnectorConf.DefaultQueryRetryDelay
  }

  it should "resolve constant retry delay settings" in {
    val sparkConf = new SparkConf(loadDefaults = false)
    sparkConf.set("spark.cassandra.query.retry.delay", "1234")

    val connConf = CassandraConnectorConf(sparkConf)
    connConf.queryRetryDelay shouldBe RetryDelayConf.ConstantDelay(1234 milliseconds)
  }

  it should "resolve linear retry delay settings" in {
    val sparkConf = new SparkConf(loadDefaults = false)
    sparkConf.set("spark.cassandra.query.retry.delay", "1234+2000")

    val connConf = CassandraConnectorConf(sparkConf)
    connConf.queryRetryDelay shouldBe RetryDelayConf.LinearDelay(1234 milliseconds, 2000 milliseconds)
  }

  it should "resolve exponential retry delay settings" in {
    val sparkConf = new SparkConf(loadDefaults = false)
    sparkConf.set("spark.cassandra.query.retry.delay", "1234*2.3")

    val connConf = CassandraConnectorConf(sparkConf)
    connConf.queryRetryDelay shouldBe RetryDelayConf.ExponentialDelay(1234 milliseconds, 2.3d)
  }

}
