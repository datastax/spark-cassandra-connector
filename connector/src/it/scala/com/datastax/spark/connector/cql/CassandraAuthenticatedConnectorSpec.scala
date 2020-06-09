package com.datastax.spark.connector.cql

import java.io.IOException

import com.datastax.oss.driver.api.core.AllNodesFailedException
import com.datastax.oss.driver.api.core.auth.AuthenticationException
import com.datastax.spark.connector.cluster.AuthCluster
import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, _}

import scala.collection.JavaConverters._

class CassandraAuthenticatedConnectorSpec extends SparkCassandraITFlatSpecBase with AuthCluster {


  val authConf = defaultConf
  val defaultConnConf = CassandraConnectorConf(authConf)
  val defaultContactInfo = defaultConnConf.contactInfo.asInstanceOf[IpBasedContactInfo]


  "A CassandraConnector" should "authenticate with username and password when using native protocol for valid credentials provided by AuthCluster" in {
    val conn2 = CassandraConnector(authConf)
    conn2.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
    }
  }

  it should "authenticate valid username/password for provided credentials" in {
    val conn2 = new CassandraConnector(defaultConnConf.copy(
      contactInfo = defaultContactInfo.copy(authConf = PasswordAuthConf("cassandra", "cassandra"))
    ))
    conn2.withSessionDo { session => assert(session !== null) }
  }

  it should "fail to authenticate invalid username/password" in {
    val conn2 = new CassandraConnector(defaultConnConf.copy(
      contactInfo = defaultContactInfo.copy(authConf = PasswordAuthConf("cassandra", "wrong_passoword"))
    ))
    val exception = intercept[IOException] {
      conn2.withSessionDo { session => assert(session !== null) }
    }
    exception.getCause shouldBe a[AllNodesFailedException]
    exception.getCause.asInstanceOf[AllNodesFailedException]
      .getAllErrors.values().asScala
      .head.asScala
      .head shouldBe a[AuthenticationException]
  }

  "A DataFrame" should "read and write data with valid auth" in {

    spark.conf.set(DefaultAuthConfFactory.UserNameParam.name, "cassandra")
    spark.conf.set(DefaultAuthConfFactory.PasswordParam.name, "cassandra")

    val conn = CassandraConnector(authConf)

    val personDF1 = spark.createDataFrame(Seq(
      ("Andy", 28, "America"),
      ("Kaushal", 25, "India"),
      ("Desanth", 27, "India"),
      ("Mahendra", 26, "Rajasthan"))).toDF("name", "age", "address")

    createKeyspace(conn.openSession())
    personDF1.createCassandraTable(ks, "authtest", Some(Array("address")), Some(Array("age")))(conn)

    val options = Map("spark.cassandra.auth.username" -> "cassandra",
      "spark.cassandra.auth.password" -> "cassandra",
      "keyspace" -> ks, "table" -> "authtest")

    personDF1.write.format("org.apache.spark.sql.cassandra").options(options).mode("append")save()
    val personDF2 = spark.read.format("org.apache.spark.sql.cassandra").options(options).load()

    personDF2.count should be(4)
  }
}
