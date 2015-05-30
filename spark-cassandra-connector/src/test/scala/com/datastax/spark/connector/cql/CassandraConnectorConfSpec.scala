package com.datastax.spark.connector.cql

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.SparkConf
import org.scalatest.{Matchers, FlatSpec}

class CassandraConnectorConfSpec extends FlatSpec with Matchers {

  it should "be serializable" in {
    val conf = CassandraConnectorConf(new SparkConf)
    SerializationUtils.roundtrip(conf)
  }

}
