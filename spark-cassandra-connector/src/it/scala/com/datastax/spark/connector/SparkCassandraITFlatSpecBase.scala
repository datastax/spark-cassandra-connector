package com.datastax.spark.connector

import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.testkit.{AbstractSpec, SharedEmbeddedCassandra}
import org.apache.spark.SparkContext
import org.scalatest._


trait SparkCassandraITFlatSpecBase extends FlatSpec with SparkCassandraITSpecBase

trait SparkCassandraITWordSpecBase extends WordSpec with SparkCassandraITSpecBase

trait SparkCassandraITAbstractSpecBase extends AbstractSpec with SparkCassandraITSpecBase

trait SparkCassandraITSpecBase extends Suite with Matchers with SharedEmbeddedCassandra with SparkTemplate with BeforeAndAfterAll {
}
