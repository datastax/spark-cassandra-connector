package com.datastax.spark.connector.demo

import com.datastax.spark.connector.cql.CassandraConnector

object BasicReadWriteDemo extends DemoApp {

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.key_value (key INT PRIMARY KEY, value VARCHAR)")
    session.execute("TRUNCATE test.key_value")
    session.execute("INSERT INTO test.key_value(key, value) VALUES (1, 'first row')")
    session.execute("INSERT INTO test.key_value(key, value) VALUES (2, 'second row')")
    session.execute("INSERT INTO test.key_value(key, value) VALUES (3, 'third row')")
  }

  import com.datastax.spark.connector._

  // Read table test.kv and print its contents:
  val rdd = sc.cassandraTable("test", "key_value").select("key", "value")
  rdd.collect().foreach(row => log.info(s"Existing Data: $row"))

  // Write two new rows to the test.kv table:
  val col = sc.parallelize(Seq((4, "fourth row"), (5, "fifth row")))
  col.saveToCassandra("test", "key_value", SomeColumns("key", "value"))

  // Assert the two new rows were stored in test.kv table:
  assert(col.collect().length == 2)

  col.collect().foreach(row => log.info(s"New Data: $row"))
  log.info(s"Work completed, stopping the Spark context.")
  sc.stop()
}
