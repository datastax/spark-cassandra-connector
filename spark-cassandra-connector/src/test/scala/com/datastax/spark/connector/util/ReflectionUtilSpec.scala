package com.datastax.spark.connector.util

import com.datastax.spark.connector.cql.{CassandraConnectorConf, DefaultConnectionFactory, CassandraConnectionFactory}
import org.scalatest.{FlatSpec, Matchers}

class ReflectionUtilSpec extends FlatSpec with Matchers {

  "ReflectionUtil.findGlobalObject" should "be able to find DefaultConnectionFactory" in {
    val factory = ReflectionUtil.findGlobalObject[CassandraConnectionFactory](
      "com.datastax.spark.connector.cql.DefaultConnectionFactory")
    factory should be(DefaultConnectionFactory)
  }

  it should "be able to instantiate a singleton object based on Java class name" in {
    val obj = ReflectionUtil.findGlobalObject[String]("java.lang.String")
    obj should be ("")
  }

  it should "cache Java class instances" in {
    val obj1 = ReflectionUtil.findGlobalObject[String]("java.lang.String")
    val obj2 = ReflectionUtil.findGlobalObject[String]("java.lang.String")
    obj1 shouldBe theSameInstanceAs (obj2)
  }

  it should "throw IllegalArgumentException when asked for a Scala object of wrong type" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[CassandraConnectorConf](
        "com.datastax.spark.connector.cql.DefaultConnectionFactory")
    }
  }

  it should "throw IllegalArgumentException when asked for class instance of wrong type" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[Integer]("java.lang.String")
    }
  }

  it should "throw IllegalArgumentException when object does not exist" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[CassandraConnectorConf]("NoSuchObject")
    }
  }

}
