package com.datastax.spark.connector.cql

import com.datastax.spark.connector._
import com.datastax.spark.connector.embedded._

class CassandraPartitionKeyWhereSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS where_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS where_test.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
    session.execute("INSERT INTO where_test.key_value (key, group, value) VALUES (1, 100, '0001')")
    session.execute("INSERT INTO where_test.key_value (key, group, value) VALUES (2, 200, '0002')")
    session.execute("INSERT INTO where_test.key_value (key, group, value) VALUES (3, 300, '0003')")
    session.execute("CREATE TABLE IF NOT EXISTS where_test.ckey_value (key1 INT, \"Key2\" BIGINT, group INT, value TEXT, PRIMARY KEY ((key1, \"Key2\"), group))")
    session.execute("INSERT INTO where_test.ckey_value (key1, \"Key2\", group, value) VALUES (1, 100, 1000, '0001')")
    session.execute("INSERT INTO where_test.ckey_value (key1, \"Key2\", group, value) VALUES (2, 200, 2000, '0002')")
    session.execute("INSERT INTO where_test.ckey_value (key1, \"Key2\", group, value) VALUES (3, 300, 3000, '0003')")

  }

  "A CassandraRDD" should "allow partition key eq in where" in {
    val rdd = sc.cassandraTable("where_test", "key_value").where("key = ?", 1)
    val result =  rdd.collect()
    result should have length 1
    result.head.getInt("key") should be (1)
  }

  it should "allow partition key 'in' in where" in {
    val result = sc.cassandraTable("where_test", "key_value").where("key in (?, ?)", 2,3).collect()
    result should have length 2
    result.head.getInt("key") should (be (2) or be (3))
  }

  it should "allow cluster key 'in' in where" in {
    val result = sc.cassandraTable("where_test", "key_value").where("group in (?, ?)", 200,300).collect()
    result should have length 2
    result.head.getInt("key") should (be (2) or be (3))
  }

  it should "work with composite keys in" in {
    val result = sc.cassandraTable("where_test", "ckey_value").where("key1 = 1 and \"Key2\" in (?, ?)", 100,200).collect()
    result should have length 1
    result.head.getInt("key1") should be (1)
  }

  it should "work with composite keys eq" in {
    val result = sc.cassandraTable("where_test", "ckey_value").where("key1 = ? and \"Key2\" = ?", 1,100).collect()
    result should have length 1
    result.head.getInt("key1") should be (1)
  }

  it should "work with composite keys in2" in {
    val result = sc.cassandraTable("where_test", "ckey_value").where("\"Key2\" in (?, ?) and key1 = 1", 100,200).collect()
    result should have length 1
    result.head.getInt("key1") should be (1)
  }
}
