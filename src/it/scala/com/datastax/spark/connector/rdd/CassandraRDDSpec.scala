package com.datastax.spark.connector.rdd

import java.io.IOException
import java.net.InetAddress

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util.{CassandraServer, SparkServer}
import org.scalatest.{FlatSpec, Matchers}


case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithConversion(key: String, group: Int, value: Long)


class MutableKeyValue(var key: Int, var group: Long) extends Serializable {
  var value: String = null
}

class MutableKeyValueWithConversion(var key: String, var group: Int) extends Serializable {
  var value: Long = 0L
}

class CassandraRDDSpec extends FlatSpec with Matchers with CassandraServer  with SparkServer {

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(InetAddress.getByName("127.0.0.1"))

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS read_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE read_test.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
    session.execute("INSERT INTO read_test.key_value (key, group, value) VALUES (1, 100, '0001')")
    session.execute("INSERT INTO read_test.key_value (key, group, value) VALUES (2, 100, '0002')")
    session.execute("INSERT INTO read_test.key_value (key, group, value) VALUES (3, 300, '0003')")

    session.execute("CREATE TABLE read_test.collections (key INT PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)")
    session.execute("INSERT INTO read_test.collections (key, l, s, m) VALUES (1, ['item1', 'item2'], {'item1', 'item2'}, {'key1': 'value1', 'key2': 'value2'})")
    session.execute("INSERT INTO read_test.collections (key, l, s, m) VALUES (2, null, null, null)")

    session.execute("CREATE TABLE read_test.blobs (key INT PRIMARY KEY, b blob)")
    session.execute("INSERT INTO read_test.blobs (key, b) VALUES (1, 0x0102030405060708090a0b0c)")
    session.execute("INSERT INTO read_test.blobs (key, b) VALUES (2, null)")

    session.execute("CREATE TABLE read_test.composite_key (key_c1 INT, key_c2 INT, group INT, value TEXT, PRIMARY KEY ((key_c1, key_c2), group))")
    session.execute("INSERT INTO read_test.composite_key (key_c1, key_c2, group, value) VALUES (1, 1, 1, 'value1')")
    session.execute("INSERT INTO read_test.composite_key (key_c1, key_c2, group, value) VALUES (1, 1, 2, 'value2')")
    session.execute("INSERT INTO read_test.composite_key (key_c1, key_c2, group, value) VALUES (1, 2, 3, 'value3')")
    session.execute("INSERT INTO read_test.composite_key (key_c1, key_c2, group, value) VALUES (2, 2, 4, 'value4')")
  }

  "A CassandraRDD" should "allow to read a Cassandra table as Array of CassandraRow" in {
    val result = sc.cassandraTable("read_test", "key_value").toArray()
    result should have length 3
    result.head.getInt("key") should (be >= 1 and be <= 3)
    result.head.getLong("group") should (be >= 100L and be <= 300L)
    result.head.getString("value") should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of tuples" in {
    val result = sc.cassandraTable[(Int, Long, String)]("read_test", "key_value").toArray()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
    result.head._3 should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class objects" in {
    val result = sc.cassandraTable[KeyValue]("read_test", "key_value").toArray()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.group should (be >= 100L and be <= 300L)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined mutable objects" in {
    val result = sc.cassandraTable[MutableKeyValue]("read_test", "key_value").toArray()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.group should (be >= 100L and be <= 300L)
    result.head.value should startWith("000")
  }

  it should "apply proper data type conversions for tuples" in {
    val result = sc.cassandraTable[(String, Int, Long)]("read_test", "key_value").toArray()
    result should have length 3
    Some(result.head._1) should contain oneOf("1", "2", "3")
    result.head._2 should (be >= 100 and be <= 300)
    result.head._3 should (be >= 1L and be <= 3L)
  }

  it should "apply proper data type conversions for user-defined case class objects" in {
    val result = sc.cassandraTable[KeyValueWithConversion]("read_test", "key_value").toArray()
    result should have length 3
    Some(result.head.key) should contain oneOf("1", "2", "3")
    result.head.group should (be >= 100 and be <= 300)
    result.head.value should (be >= 1L and be <= 3L)
  }

  it should "apply proper data type conversions for user-defined mutable objects" in {
    val result = sc.cassandraTable[MutableKeyValueWithConversion]("read_test", "key_value").toArray()
    result should have length 3
    Some(result.head.key) should contain oneOf("1", "2", "3")
    result.head.group should (be >= 100 and be <= 300)
    result.head.value should (be >= 1L and be <= 3L)
  }

  it should "map columns to objects using user-defined function" in {
    val result = sc.cassandraTable[MutableKeyValue]("read_test", "key_value")
      .as((key: Int, group: Long, value: String) => (key, group, value)).toArray()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
    result.head._3 should startWith("000")
  }

  it should "map columns to objects using user-defined function with type conversion" in {
    val result = sc.cassandraTable[MutableKeyValue]("read_test", "key_value")
      .as((key: String, group: String, value: Option[String]) => (key, group, value)).toArray()
    result should have length 3
    Some(result.head._1) should contain oneOf("1", "2", "3")
    Some(result.head._2) should contain oneOf("100", "300")
    Some(result.head._3) should contain oneOf(Some("0001"), Some("0002"), Some("0003"))
  }

  it should "allow for selecting a subset of columns" in {
    val result = sc.cassandraTable("read_test", "key_value").select("value").toArray()
    result should have length 3
    result.head.size shouldEqual 1
    result.head.getString("value") should startWith("000")
  }

  it should "allow for selecting a subset of rows" in {
    val result = sc.cassandraTable("read_test", "key_value").where("group < ?", 200L).toArray()
    result should have length 2
    result.head.size shouldEqual 3
    result.head.getInt("group") shouldEqual 100
    result.head.getString("value") should startWith("000")
  }

  it should "use a single partition for a tiny table" in {
    val rdd = sc.cassandraTable("read_test", "key_value")
    rdd.partitions should have length 1
  }

  it should "allow for reading collections" in {
    val result = sc.cassandraTable("read_test", "collections").toArray()
    val rowById = result.groupBy(_.getInt("key")).mapValues(_.head)
    rowById(1).getList[String]("l") shouldEqual Vector("item1", "item2")
    rowById(1).getSet[String]("s") shouldEqual Set("item1", "item2")
    rowById(1).getMap[String, String]("m") shouldEqual Map("key1" -> "value1", "key2" -> "value2")

    rowById(2).getList[String]("l") shouldEqual Vector.empty
    rowById(2).getSet[String]("s") shouldEqual Set.empty
    rowById(2).getMap[String, String]("m") shouldEqual Map.empty
  }

  it should "allow for reading blobs" in {
    val result = sc.cassandraTable("read_test", "blobs").toArray()
    val rowById = result.groupBy(_.getInt("key")).mapValues(_.head)
    rowById(1).getBytes("b").limit() shouldEqual 12
    rowById(1).get[Array[Byte]]("b") shouldEqual Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    rowById(2).getBytesOption("b") shouldEqual None
  }

  it should "allow for reading tables with composite partitioning key" in {
    val result = sc.cassandraTable[(Int, Int, Int, String)]("read_test", "composite_key").where("group >= ?", 3).toArray()
    result should have length 2
  }

  it should "throw IOException when table could not be found" in {
    intercept[IOException] { sc.cassandraTable("read_test", "unknown_table").toArray() }
  }

}
