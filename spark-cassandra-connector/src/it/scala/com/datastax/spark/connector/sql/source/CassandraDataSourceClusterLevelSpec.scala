package com.datastax.spark.connector.sql.source

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector}
import com.datastax.spark.connector.embedded.EmbeddedCassandra._
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.cassandra.CassandraDefaultSource._

class CassandraDataSourceClusterLevelSpec  extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template", "cassandra-default.yaml.template"))
  val conn = CassandraConnector(Set(getHost(0)))

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS sql_test1 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test1.test1 (a INT PRIMARY KEY, b INT, c INT)")
    session.execute("USE sql_test1")
    session.execute("INSERT INTO sql_test1.test1 (a, b, c) VALUES (1, 1, 1)")
    session.execute("INSERT INTO sql_test1.test1 (a, b, c) VALUES (2, 1, 2)")
    session.execute("INSERT INTO sql_test1.test1 (a, b, c) VALUES (3, 1, 3)")
    session.execute("INSERT INTO sql_test1.test1 (a, b, c) VALUES (4, 1, 4)")
    session.execute("INSERT INTO sql_test1.test1 (a, b, c) VALUES (5, 1, 5)")
  }

  val conn2 = CassandraConnector(Set(getHost(1)), getNativePort(1))
  conn2.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS sql_test2 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test2.test2 (a INT PRIMARY KEY, d INT, e INT)")
    session.execute("USE sql_test2")
    session.execute("INSERT INTO sql_test2.test2 (a, d, e) VALUES (8, 1, 8)")
    session.execute("INSERT INTO sql_test2.test2 (a, d, e) VALUES (7, 1, 7)")
    session.execute("INSERT INTO sql_test2.test2 (a, d, e) VALUES (6, 1, 6)")
    session.execute("INSERT INTO sql_test2.test2 (a, d, e) VALUES (4, 1, 4)")
    session.execute("INSERT INTO sql_test2.test2 (a, d, e) VALUES (5, 1, 5)")
    session.execute("CREATE TABLE IF NOT EXISTS sql_test2.test3 (a INT PRIMARY KEY, d INT, e INT)")
  }

  val sqlContext: SQLContext = new SQLContext(sc)
  def pushDown: Boolean = true

  override def beforeAll() {
    val conf1 = new SparkConf(true)
      .set("spark.cassandra.connection.host", getHost(0).getHostAddress)
      .set("spark.cassandra.connection.native.port", getNativePort(0).toString)
      .set("spark.cassandra.connection.rpc.port", getRpcPort(0).toString)
    sqlContext.addCassandraConnConf(CassandraConnectorConf(conf1), "cluster1")
    val conf2 = new SparkConf(true)
      .set("spark.cassandra.connection.host", getHost(1).getHostAddress)
      .set("spark.cassandra.connection.native.port", getNativePort(1).toString)
      .set("spark.cassandra.connection.rpc.port", getRpcPort(1).toString)
    sqlContext.addCassandraConnConf(CassandraConnectorConf(conf2), "cluster2")
    sqlContext.addClusterLevelReadConf(ReadConf.fromSparkConf(sc.getConf), "cluster1")
    sqlContext.addClusterLevelWriteConf(WriteConf.fromSparkConf(sc.getConf), "cluster1")
    sqlContext.addClusterLevelReadConf(ReadConf.fromSparkConf(sc.getConf), "cluster2")
    sqlContext.addClusterLevelWriteConf(WriteConf.fromSparkConf(sc.getConf), "cluster2")

    createTempTable("sql_test1", "test1", "cluster1", "test1")
    createTempTable("sql_test2", "test2", "cluster2", "test2")
    createTempTable("sql_test2", "test3", "cluster2", "test3")
  }

  override def afterAll() {
    super.afterAll()
    conn.withSessionDo { session =>
      session.execute("DROP KEYSPACE sql_test1")
    }
    conn2.withSessionDo { session =>
      session.execute("DROP KEYSPACE sql_test2")
    }
    sqlContext.dropTempTable("test1")
    sqlContext.dropTempTable("test2")
    sqlContext.dropTempTable("test3")
  }

  def createTempTable(keyspace: String, table: String, cluster: String, tmpTable: String) = {
    sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE $tmpTable
        |USING org.apache.spark.sql.cassandra
        |OPTIONS (
        | c_table "$table",
        | keyspace "$keyspace",
        | cluster "$cluster",
        | push_down "$pushDown"
        | )
      """.stripMargin.replaceAll("\n", " "))
  }

  it should "allow to join tables from different clusters" in {
    val result = sqlContext.sql("SELECT * FROM test1 Join test2 where test1.a=test2.a").collect()
    result should have length 2
  }

  it should "allow to write data to another cluster" in {
    val insert = sqlContext.sql("INSERT INTO TABLE test3 SELECT * FROM test1").collect()
    val result = sqlContext.sql("SELECT * FROM test3").collect()
    result should have length 5
  }
}
