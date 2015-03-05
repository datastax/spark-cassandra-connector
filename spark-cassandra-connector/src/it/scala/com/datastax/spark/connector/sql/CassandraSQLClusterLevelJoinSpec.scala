package com.datastax.spark.connector.sql

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SecondEmbeddedCassandra, SparkTemplate}
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.scalatest.{Matchers, FlatSpec}

class CassandraSQLClusterLevelJoinSpec extends FlatSpec with Matchers with SharedEmbeddedCassandra with SecondEmbeddedCassandra with SparkTemplate {
  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(Set(cassandraHost))
  conf.set("spark.cluster1.cassandra.connection.host", EmbeddedCassandra.cassandraHost.getHostAddress)
    .set("spark.cluster1.cassandra.connection.native.port", "9042")
    .set("spark.cluster1.cassandra.connection.rpc.port", "9160")
    .set("spark.cluster2.cassandra.connection.host", SecondEmbeddedCassandra.cassandraHost.getHostAddress)
    .set("spark.cluster2.cassandra.connection.native.port", "9043")
    .set("spark.cluster2.cassandra.connection.rpc.port", "9161")
    .set("spark.driver.allowMultipleContexts", "true")
  val sparkContext = new SparkContext(conf)
  val cc = new CassandraSQLContext(sparkContext)

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

  useCassandraConfig2("cassandra-second.yaml.template")
  val conn2 = CassandraConnector(Set(cassandraHost2), 9043)
  conn2.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS sql_test2 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test2.test2 (a INT PRIMARY KEY, d INT, e INT)")
    session.execute("USE sql_test2")
    session.execute("INSERT INTO sql_test2.test2 (a, d, e) VALUES (8, 1, 8)")
    session.execute("INSERT INTO sql_test2.test2 (a, d, e) VALUES (7, 1, 7)")
    session.execute("INSERT INTO sql_test2.test2 (a, d, e) VALUES (6, 1, 6)")
    session.execute("INSERT INTO sql_test2.test2 (a, d, e) VALUES (4, 1, 4)")
    session.execute("INSERT INTO sql_test2.test2 (a, d, e) VALUES (5, 1, 5)")
  }

  it should "allow to join tables from different clusters" in {
    val result = cc.sql("SELECT * FROM cluster1.sql_test1.test1 AS test1 Join cluster2.sql_test2.test2 AS test2 where test1.a=test2.a").collect()
    result should have length 2
  }
}
