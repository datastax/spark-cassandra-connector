package com.datastax.spark.connector.writer

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.{Schema, CassandraConnector}
import com.datastax.spark.connector.embedded.SparkTemplate._

import com.datastax.driver.core.ProtocolVersion

class BoundStatementBuilderSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(defaultConf)

  val ks = "bsb_ks"

  conn.withSessionDo { session =>
      session.execute(s"""CREATE KEYSPACE IF NOT EXISTS "$ks" WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }""")
      session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".tab (id INT PRIMARY KEY, value TEXT)""")
  }
  val cluster = conn.withClusterDo( c => c)

  val schema = Schema.fromCassandra(conn, Some(ks), Some("tab"))
  val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, Option[String])].rowWriter(schema.tables.head, IndexedSeq("id", "value"))
  val rkg = new RoutingKeyGenerator(schema.tables.head, Seq("id", "value"))
  val ps = conn.withSessionDo(session =>
    session.prepare(s"""INSERT INTO "$ks".tab (id, value) VALUES (?, ?) """))

  val protocolVersion = conn.withClusterDo(cluster => cluster.getConfiguration.getProtocolOptions.getProtocolVersion)

  "BoundStatementBuilder" should "ignore None values if ProtocolVersion > 3" in {
      val bsb = new BoundStatementBuilder(rowWriter, ps, protocolVersion = ProtocolVersion.V4)
      val x = bsb.bind((1,None))

      x.isSet("value") should be (false)
  }

  it should "set None values to null if ProtocolVersion <= 3" in {
      val bsb = new BoundStatementBuilder(rowWriter, ps, protocolVersion = ProtocolVersion.V3)
      val x = bsb.bind((1, None))

      x.isNull("value") should be (true)
  }
}
