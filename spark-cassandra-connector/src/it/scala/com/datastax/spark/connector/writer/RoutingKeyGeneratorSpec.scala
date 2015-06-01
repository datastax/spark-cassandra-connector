package com.datastax.spark.connector.writer

import scala.collection.immutable.Map

import org.apache.cassandra.dht.IPartitioner

import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.{CassandraRow, SparkCassandraITFlatSpecBase}

class RoutingKeyGeneratorSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  val ks = "RoutingKeyGeneratorSpec"

  conn.withSessionDo { session =>
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS "$ks" WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".one_key (id INT PRIMARY KEY, value TEXT)""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".two_keys (id INT, id2 TEXT, value TEXT, PRIMARY KEY ((id, id2)))""")
  }

  implicit val protocolVersion = conn.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersionEnum)
  val cp = conn.withClusterDo(cluster => Class.forName(cluster.getMetadata.getPartitioner).newInstance().asInstanceOf[IPartitioner])

  "RoutingKeyGenerator" should "generate proper routing keys when there is one partition key column" in {
    val schema = Schema.fromCassandra(conn, Some(ks), Some("one_key"))
    val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, String)].rowWriter(schema.tables.head, IndexedSeq("id", "value"))
    val rkg = new RoutingKeyGenerator(schema.tables.head, Seq("id", "value"))

    conn.withSessionDo { session =>
      val pStmt = session.prepare(s"""INSERT INTO "$ks".one_key (id, value) VALUES (:id, :value)""")
      val bStmt = pStmt.bind(1: java.lang.Integer, "first row")

      session.execute(bStmt)
      val row = session.execute(s"""SELECT TOKEN(id) FROM "$ks".one_key WHERE id = 1""").one()

      val readTokenStr = CassandraRow.fromJavaDriverRow(row, Array("token(id)")).getString(0)

      val rk = rkg.apply(bStmt)
      val rkToken = cp.getToken(rk)

      rkToken.getTokenValue.toString should be(readTokenStr)
    }
  }

  "RoutingKeyGenerator" should "generate proper routing keys when there are more partition key columns" in {
    val schema = Schema.fromCassandra(conn, Some(ks), Some("two_keys"))
    val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, String, String)].rowWriter(schema.tables.head, IndexedSeq("id", "id2", "value"))
    val rkg = new RoutingKeyGenerator(schema.tables.head, Seq("id", "id2", "value"))

    conn.withSessionDo { session =>
      val pStmt = session.prepare(s"""INSERT INTO "$ks".two_keys (id, id2, value) VALUES (:id, :id2, :value)""")
      val bStmt = pStmt.bind(1: java.lang.Integer, "one", "first row")

      session.execute(bStmt)
      val row = session.execute(s"""SELECT TOKEN(id, id2) FROM "$ks".two_keys WHERE id = 1 AND id2 = 'one'""").one()

      val readTokenStr = CassandraRow.fromJavaDriverRow(row, Array("token(id,id2)")).getString(0)

      val rk = rkg.apply(bStmt)
      val rkToken = cp.getToken(rk)

      rkToken.getTokenValue.toString should be(readTokenStr)
    }
  }

}
