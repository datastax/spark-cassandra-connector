package com.datastax.spark.connector.demo

import com.datastax.spark.connector.cql.CassandraConnector

object TableCopyDemo extends DemoApp {

  import com.datastax.spark.connector._

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.source (key INT PRIMARY KEY, data VARCHAR)")
    session.execute("CREATE TABLE IF NOT EXISTS test.destination (key INT PRIMARY KEY, data VARCHAR)")
    session.execute("TRUNCATE test.source")
    session.execute("TRUNCATE test.destination")
    session.execute("INSERT INTO test.source(key, data) VALUES (1, 'first row')")
    session.execute("INSERT INTO test.source(key, data) VALUES (2, 'second row')")
    session.execute("INSERT INTO test.source(key, data) VALUES (3, 'third row')")
  }

  val src = sc.cassandraTable("test", "source")
  src.saveToCassandra("test", "destination")

  val dest = sc.cassandraTable("test", "destination")
  dest.collect().foreach(row => log.debug(s"$row"))
}
