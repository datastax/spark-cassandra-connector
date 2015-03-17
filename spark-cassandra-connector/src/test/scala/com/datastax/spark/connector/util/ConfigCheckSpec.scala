package com.datastax.spark.connector.util

import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class ConfigCheckSpec extends FlatSpec with Matchers  {

  "ConfigCheck" should " throw an exception when the configuration contains a invalid spark.cassandra prop" in {
    val sparkConf = new SparkConf().set("spark.cassandra.foo.bar", "foobar")
    val exception = the [ConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage should include ("spark.cassandra.foo.bar")
  }

  it should " suggest alternatives if you have a slight misspelling " in {
    val sparkConf = new SparkConf()
      .set("spark.cassandra.output.batch.siz.bytez", "40")
      .set("spark.cassandra.output.batch.size.row","10")
      .set("spark.cassandra.connect.host", "123.231.123.231")

    val exception = the[ConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage should include("spark.cassandra.output.batch.size.bytes")
    exception.getMessage should include("spark.cassandra.output.batch.size.rows")
    exception.getMessage should include("spark.cassandra.connection.host")
  }

  it should " suggest alternatives if you miss a word " in {
    val sparkConf = new SparkConf()
      .set("spark.cassandra.output.batch.bytez", "40")
      .set("spark.cassandra.output.size.row","10")
      .set("spark.cassandra.host", "123.231.123.231")

    val exception = the[ConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage should include("spark.cassandra.output.batch.size.bytes")
    exception.getMessage should include("spark.cassandra.output.batch.size.rows")
    exception.getMessage should include("spark.cassandra.connection.host")
  }

  it should " not throw an exception if you have a random variable not in the spark.cassandra space" in {
    val sparkConf = new SparkConf()
      .set("my.own.var", "40")
      .set("spark.cassandraOther.var","42")
    ConfigCheck.checkConfig(sparkConf)
  }

  it should " not list all options as suggestions " in {
     val sparkConf = new SparkConf()
      .set("spark.cassandra.output.batch.bytez", "40")
    val exception = the[ConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage shouldNot include ("connection")
    exception.getMessage shouldNot include ("input")
  }

  it should " not give suggestions when the variable is very strange " in {
    val sparkConf = new SparkConf().set("spark.cassandra.foo.bar", "foobar")
    val exception = the [ConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage shouldNot include ("Possible matches")
  }

}
