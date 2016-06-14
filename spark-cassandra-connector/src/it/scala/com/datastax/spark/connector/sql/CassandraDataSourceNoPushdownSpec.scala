package com.datastax.spark.connector.sql

import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.SaveMode._

import org.apache.spark.sql.cassandra.{TableRef, CassandraSourceRelation}

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate._

class CassandraDataSourceNoPushdownSpec extends CassandraDataSourceSpec{
  override def pushDown: Boolean = false
}
