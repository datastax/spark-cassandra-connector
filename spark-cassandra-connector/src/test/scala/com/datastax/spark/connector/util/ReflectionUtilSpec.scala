package com.datastax.spark.connector.util

import com.datastax.spark.connector.cql.{CassandraConnectorConf, DefaultConnectionFactory, CassandraConnectionFactory}
import org.scalatest.{FlatSpec, Matchers}

class ReflectionUtilSpec extends FlatSpec with Matchers {

  "ReflectionUtil.findGlobalObject" should "be able to find DefaultConnectionFactory" in {
    val factory = ReflectionUtil.findGlobalObject[CassandraConnectionFactory](
      "com.datastax.spark.connector.cql.DefaultConnectionFactory")
    factory should be(DefaultConnectionFactory)
  }

  it should "throw IllegalArgumentException when asked for an object of wrong type" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[CassandraConnectorConf](
        "com.datastax.spark.connector.cql.DefaultConnectionFactory")
    }
  }

  it should "throw IllegalArgumentException when object does not exist" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[CassandraConnectorConf]("NoSuchObject")
    }
  }
}
