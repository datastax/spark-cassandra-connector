package com.datastax.spark.connector.writer

import com.datastax.spark.connector.{SparkCassandraITFlatSpecBase, CassandraRow}
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import org.apache.cassandra.dht.IPartitioner

import scala.collection.immutable.Map

class RoutingKeyGeneratorSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS routing_key_gen_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS routing_key_gen_test.one_key (id INT PRIMARY KEY, value TEXT)")
    session.execute("CREATE TABLE IF NOT EXISTS routing_key_gen_test.two_keys (id INT, id2 TEXT, value TEXT, PRIMARY KEY ((id, id2)))")
  }

  implicit val protocolVersion = conn.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersionEnum)
  val cp = conn.withClusterDo(cluster => Class.forName(cluster.getMetadata.getPartitioner).newInstance().asInstanceOf[IPartitioner])

  "RoutingKeyGenerator" should "generate proper routing keys when there is one partition key column" in {
    val schema = Schema.fromCassandra(conn, Some("routing_key_gen_test"), Some("one_key"))
    val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, String)].rowWriter(schema.tables.head, Seq("id", "value"), Map.empty)
    val rkg = new RoutingKeyGenerator(schema.tables.head, Seq("id", "value"))

    conn.withSessionDo { session =>
      val pStmt = session.prepare("INSERT INTO routing_key_gen_test.one_key (id, value) VALUES (:id, :value)")
      val bStmt = pStmt.bind(1: java.lang.Integer, "first row")

      session.execute(bStmt)
      val row = session.execute("SELECT TOKEN(id) FROM routing_key_gen_test.one_key WHERE id = 1").one()

      val readTokenStr = CassandraRow.fromJavaDriverRow(row, Array("token(id)")).getString(0)

      val rk = rkg.apply(bStmt)
      val rkToken = cp.getToken(rk)

      rkToken.getTokenValue.toString should be(readTokenStr)
    }
  }

  "RoutingKeyGenerator" should "generate proper routing keys when there are more partition key columns" in {
    val schema = Schema.fromCassandra(conn, Some("routing_key_gen_test"), Some("two_keys"))
    val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, String, String)].rowWriter(schema.tables.head, Seq("id", "id2", "value"), Map.empty)
    val rkg = new RoutingKeyGenerator(schema.tables.head, Seq("id", "id2", "value"))

    conn.withSessionDo { session =>
      val pStmt = session.prepare("INSERT INTO routing_key_gen_test.two_keys (id, id2, value) VALUES (:id, :id2, :value)")
      val bStmt = pStmt.bind(1: java.lang.Integer, "one", "first row")

      session.execute(bStmt)
      val row = session.execute("SELECT TOKEN(id, id2) FROM routing_key_gen_test.two_keys WHERE id = 1 AND id2 = 'one'").one()

      val readTokenStr = CassandraRow.fromJavaDriverRow(row, Array("token(id,id2)")).getString(0)

      val rk = rkg.apply(bStmt)
      val rkToken = cp.getToken(rk)

      rkToken.getTokenValue.toString should be(readTokenStr)
    }
  }

}
