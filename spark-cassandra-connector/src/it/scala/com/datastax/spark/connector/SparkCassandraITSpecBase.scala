package com.datastax.spark.connector

import com.datastax.spark.connector.embedded.{SparkTemplate}
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.apache.spark.SparkContext
import org.scalatest.{Matchers, FlatSpec, ConfigMap, BeforeAndAfterAll}


trait SparkCassandraITSpecBase extends FlatSpec with Matchers with SharedEmbeddedCassandra with SparkTemplate with BeforeAndAfterAll {
  var sc: SparkContext = null

  override def beforeAll(configMap: ConfigMap) {
    sc = new SparkContext(conf)
  }

  it should "initiate sc" in {
    sc should not be (null)
  }

  override def afterAll(configMap: ConfigMap) {
    if (sc != null) {
      sc.stop()
    }
  }
}
