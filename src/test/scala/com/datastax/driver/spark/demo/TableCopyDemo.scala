package com.datastax.driver.spark.demo

import com.datastax.driver.spark.connector.CassandraConnector

object TableCopyDemo extends App with DemoApp {

  import com.datastax.driver.spark._

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
  dest.toArray().foreach(println)
}
