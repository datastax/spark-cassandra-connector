package com.datastax.spark.connector.rdd

import java.io.IOException
import java.util.Date

import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.scalatest.{FlatSpec, Matchers}
import org.joda.time.DateTime
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.embedded._

import scala.reflect.runtime.universe.typeTag


case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithConversion(key: String, group: Int, value: Long)
case class CustomerId(id: String)
case class KeyGroup(key: Int, group: Int)
case class Value(value: String)

class MutableKeyValue(var key: Int, var group: Long) extends Serializable {
  var value: String = null
}

class MutableKeyValueWithConversion(var key: String, var group: Int) extends Serializable {
  var value: Long = 0L
}

class CassandraRDDSpec extends FlatSpec with Matchers with SharedEmbeddedCassandra with SparkTemplate {

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(Set(cassandraHost))
  val bigTableRowCount = 100000


  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS read_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS read_test.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
    session.execute("INSERT INTO read_test.key_value (key, group, value) VALUES (1, 100, '0001')")
    session.execute("INSERT INTO read_test.key_value (key, group, value) VALUES (2, 100, '0002')")
    session.execute("INSERT INTO read_test.key_value (key, group, value) VALUES (3, 300, '0003')")

    session.execute("CREATE TABLE IF NOT EXISTS read_test.simple_kv (key INT, value TEXT, PRIMARY KEY (key))")
    session.execute("INSERT INTO read_test.simple_kv (key, value) VALUES (1, '0001')")
    session.execute("INSERT INTO read_test.simple_kv (key, value) VALUES (2, '0002')")
    session.execute("INSERT INTO read_test.simple_kv (key, value) VALUES (3, '0003')")

    session.execute("CREATE TABLE IF NOT EXISTS read_test.collections (key INT PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)")
    session.execute("INSERT INTO read_test.collections (key, l, s, m) VALUES (1, ['item1', 'item2'], {'item1', 'item2'}, {'key1': 'value1', 'key2': 'value2'})")
    session.execute("INSERT INTO read_test.collections (key, l, s, m) VALUES (2, null, null, null)")

    session.execute("CREATE TABLE IF NOT EXISTS read_test.blobs (key INT PRIMARY KEY, b blob)")
    session.execute("INSERT INTO read_test.blobs (key, b) VALUES (1, 0x0102030405060708090a0b0c)")
    session.execute("INSERT INTO read_test.blobs (key, b) VALUES (2, null)")

    session.execute("CREATE TABLE IF NOT EXISTS read_test.composite_key (key_c1 INT, key_c2 INT, group INT, value TEXT, PRIMARY KEY ((key_c1, key_c2), group))")
    session.execute("INSERT INTO read_test.composite_key (key_c1, key_c2, group, value) VALUES (1, 1, 1, 'value1')")
    session.execute("INSERT INTO read_test.composite_key (key_c1, key_c2, group, value) VALUES (1, 1, 2, 'value2')")
    session.execute("INSERT INTO read_test.composite_key (key_c1, key_c2, group, value) VALUES (1, 2, 3, 'value3')")
    session.execute("INSERT INTO read_test.composite_key (key_c1, key_c2, group, value) VALUES (2, 2, 4, 'value4')")

    session.execute("CREATE TABLE IF NOT EXISTS read_test.clustering_time (key INT, time TIMESTAMP, value TEXT, PRIMARY KEY (key, time))")
    session.execute("INSERT INTO read_test.clustering_time (key, time, value) VALUES (1, '2014-07-12 20:00:01', 'value1')")
    session.execute("INSERT INTO read_test.clustering_time (key, time, value) VALUES (1, '2014-07-12 20:00:02', 'value2')")
    session.execute("INSERT INTO read_test.clustering_time (key, time, value) VALUES (1, '2014-07-12 20:00:03', 'value3')")

    session.execute("CREATE TYPE read_test.address (street text, city text, zip int)")
    session.execute("CREATE TABLE IF NOT EXISTS read_test.udts(key INT PRIMARY KEY, name text, addr frozen<address>)")
    session.execute("INSERT INTO read_test.udts(key, name, addr) VALUES (1, 'name', {street: 'Some Street', city: 'Paris', zip: 11120})")

    session.execute("CREATE TABLE IF NOT EXISTS read_test.big_table (key INT PRIMARY KEY, value INT)")
    val insert = session.prepare("INSERT INTO read_test.big_table(key, value) VALUES (?, ?)")
    for (i <- 1 to bigTableRowCount) {
      session.execute(insert.bind(i.asInstanceOf[AnyRef], i.asInstanceOf[AnyRef]))
    }
  }

  "A CassandraRDD" should "allow to read a Cassandra table as Array of CassandraRow" in {
    val result = sc.cassandraTable("read_test", "key_value").collect()
    result should have length 3
    result.head.getInt("key") should (be >= 1 and be <= 3)
    result.head.getLong("group") should (be >= 100L and be <= 300L)
    result.head.getString("value") should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of pairs of primitives" in {
    val result = sc.cassandraTable[(Int, Long)]("read_test", "key_value").select("key", "group").collect()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
  }

  it should "allow to read a Cassandra table as Array of tuples" in {
    val result = sc.cassandraTable[(Int, Long, String)]("read_test", "key_value").collect()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
    result.head._3 should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class objects" in {
    val result = sc.cassandraTable[KeyValue]("read_test", "key_value").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.group should (be >= 100L and be <= 300L)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined class objects" in {
    val result = sc.cassandraTable[SampleScalaClass]("read_test", "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined class (with multiple constructors) objects" in {
    val result = sc.cassandraTable[SampleScalaClassWithMultipleCtors]("read_test", "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined class (with no fields) objects" in {
    val result = sc.cassandraTable[SampleScalaClassWithNoFields]("read_test", "simple_kv").collect()
    result should have length 3
  }

  it should "allow to read a Cassandra table as Array of user-defined case class (nested) objects" in {
    val result = sc.cassandraTable[SampleWithNestedScalaCaseClass#InnerClass]("read_test", "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class (deeply nested) objects" in {
    val result = sc.cassandraTable[SampleWithDeeplyNestedScalaCaseClass#IntermediateClass#InnerClass]("read_test", "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class (nested in object) objects" in {
    val result = sc.cassandraTable[SampleObject.ClassInObject]("read_test", "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined mutable objects" in {
    val result = sc.cassandraTable[MutableKeyValue]("read_test", "key_value").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.group should (be >= 100L and be <= 300L)
    result.head.value should startWith("000")
  }

  it should "apply proper data type conversions for tuples" in {
    val result = sc.cassandraTable[(String, Int, Long)]("read_test", "key_value").collect()
    result should have length 3
    Some(result.head._1) should contain oneOf("1", "2", "3")
    result.head._2 should (be >= 100 and be <= 300)
    result.head._3 should (be >= 1L and be <= 3L)
  }

  it should "apply proper data type conversions for user-defined case class objects" in {
    val result = sc.cassandraTable[KeyValueWithConversion]("read_test", "key_value").collect()
    result should have length 3
    Some(result.head.key) should contain oneOf("1", "2", "3")
    result.head.group should (be >= 100 and be <= 300)
    result.head.value should (be >= 1L and be <= 3L)
  }

  it should "apply proper data type conversions for user-defined mutable objects" in {
    val result = sc.cassandraTable[MutableKeyValueWithConversion]("read_test", "key_value").collect()
    result should have length 3
    Some(result.head.key) should contain oneOf("1", "2", "3")
    result.head.group should (be >= 100 and be <= 300)
    result.head.value should (be >= 1L and be <= 3L)
  }

  it should "map columns to objects using user-defined function" in {
    val result = sc.cassandraTable[MutableKeyValue]("read_test", "key_value")
      .as((key: Int, group: Long, value: String) => (key, group, value)).collect()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
    result.head._3 should startWith("000")
  }

  it should "map columns to objects using user-defined function with type conversion" in {
    val result = sc.cassandraTable[MutableKeyValue]("read_test", "key_value")
      .as((key: String, group: String, value: Option[String]) => (key, group, value)).collect()
    result should have length 3
    Some(result.head._1) should contain oneOf("1", "2", "3")
    Some(result.head._2) should contain oneOf("100", "300")
    Some(result.head._3) should contain oneOf(Some("0001"), Some("0002"), Some("0003"))
  }

  it should "allow for selecting a subset of columns" in {
    val result = sc.cassandraTable("read_test", "key_value").select("value").collect()
    result should have length 3
    result.head.size shouldEqual 1
    result.head.getString("value") should startWith("000")
  }

  it should "allow for selecting a subset of rows" in {
    val result = sc.cassandraTable("read_test", "key_value").where("group < ?", 200L).collect()
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
    val result = sc.cassandraTable("read_test", "collections").collect()
    val rowById = result.groupBy(_.getInt("key")).mapValues(_.head)
    rowById(1).getList[String]("l") shouldEqual Vector("item1", "item2")
    rowById(1).getSet[String]("s") shouldEqual Set("item1", "item2")
    rowById(1).getMap[String, String]("m") shouldEqual Map("key1" -> "value1", "key2" -> "value2")

    rowById(2).getList[String]("l") shouldEqual Vector.empty
    rowById(2).getSet[String]("s") shouldEqual Set.empty
    rowById(2).getMap[String, String]("m") shouldEqual Map.empty
  }

  it should "allow for reading blobs" in {
    val result = sc.cassandraTable("read_test", "blobs").collect()
    val rowById = result.groupBy(_.getInt("key")).mapValues(_.head)
    rowById(1).getBytes("b").limit() shouldEqual 12
    rowById(1).get[Array[Byte]]("b") shouldEqual Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    rowById(2).getBytesOption("b") shouldEqual None
  }

  it should "allow for converting fields to custom types by user-defined TypeConverter" in {
    TypeConverter.registerConverter(new TypeConverter[CustomerId] {
      def targetTypeTag = typeTag[CustomerId]
      def convertPF = { case x: String => CustomerId(x) }
    })

    val result = sc.cassandraTable[(Int, Long, CustomerId)]("read_test", "key_value").collect()
    result should have length 3
    result(0)._3 shouldNot be(null)
    result(1)._3 shouldNot be(null)
    result(2)._3 shouldNot be(null)
  }

  it should "allow for reading tables with composite partitioning key" in {
    val result = sc.cassandraTable[(Int, Int, Int, String)]("read_test", "composite_key")
      .where("group >= ?", 3).collect()
    result should have length 2
  }

  it should "convert values passed to where to correct types (String -> Timestamp)" in {
    val result = sc.cassandraTable[(Int, Date, String)]("read_test", "clustering_time")
      .where("time >= ?", "2014-07-12 20:00:02").collect()
    result should have length 2
  }

  it should "convert values passed to where to correct types (DateTime -> Timestamp)" in {
    val result = sc.cassandraTable[(Int, Date, String)]("read_test", "clustering_time")
      .where("time >= ?", new DateTime(2014, 7, 12, 20, 0, 2)).collect()
    result should have length 2
  }

  it should "convert values passed to where to correct types (Date -> Timestamp)" in {
    val result = sc.cassandraTable[(Int, Date, String)]("read_test", "clustering_time")
      .where("time >= ?", new DateTime(2014, 7, 12, 20, 0, 2).toDate).collect()
    result should have length 2
  }

  it should "accept partitioning key in where" in {
    val result = sc.cassandraTable[(Int, Date, String)]("read_test", "clustering_time")
      .where("key = ?", 1).collect()
    result should have length 3
  }

  it should "accept partitioning key and clustering column predicate in where" in {
    val result = sc.cassandraTable[(Int, Date, String)]("read_test", "clustering_time")
      .where("key = ? AND time >= ?", 1, new DateTime(2014, 7, 12, 20, 0, 2).toDate).collect()
    result should have length 2
  }

  it should "accept composite partitioning key in where" in {
    val result = sc.cassandraTable[(Int, Int, Int, String)]("read_test", "composite_key")
      .where("key_c1 = ? AND key_c2 = ?", 1, 1).collect()
    result should have length 2
  }

  it should "allow to fetch columns from a table with user defined Cassandra type (UDT)" in {
    val result = sc.cassandraTable("read_test", "udts").select("key", "name").collect()
    result should have length 1
    val row = result.head
    row.getInt(0) should be(1)
    row.getString(1) should be("name")
  }

  it should "throw IOException when table could not be found" in {
    intercept[IOException] { sc.cassandraTable("read_test", "unknown_table").collect() }
  }

  it should "not leak threads" in {
    // compute a few RDDs so the thread pools get initialized
    // using parallel range, to initialize parallel collections fork-join-pools
    for (i <- (1 to 4).par)
      sc.cassandraTable("read_test", "key_value").collect()

    // subsequent computations of RDD should reuse already created thread pools,
    // not instantiate new ones
    val iterationCount = 128
    val startThreadCount = Thread.activeCount()
    for (i <- (1 to iterationCount).par)
      sc.cassandraTable("read_test", "key_value").collect()
    val endThreadCount = Thread.activeCount()

    // This is not very precise, but if there was a thread leak and we leaked even only
    // 1-thread per rdd, this test would not pass. Typically we observed the endThreadCount = startThreadCount +/- 3
    // We divide iterationCount here, in order to detect thread leaks that would not happen every time and to account
    // for a few threads exiting during the test.
    endThreadCount should be < startThreadCount + iterationCount / 2
  }

  it should "allow to read Cassandra table as Array of KV tuples of two pairs" in {
    val results = sc.cassandraTable[((Int, Int), (Int, String))]("read_test", "composite_key").select("key_c1", "key_c2" ,"group", "value").collect()
    results should have length 4
    results should contain (((1, 1), (1, "value1")))
    results should contain (((1, 1), (2, "value2")))
    results should contain (((1, 2), (3, "value3")))
    results should contain (((2, 2), (4, "value4")))
  }

  it should "allow to read Cassandra table as Array of KV tuples of a pair and a case class" in {
    val results = sc.cassandraTable[((Int, Int), Value)]("read_test", "key_value").select("key", "group", "value").collect()
    results should have length 3
    val map = results.toMap
    map((1, 100)) should be (Value("0001"))
    map((2, 100)) should be (Value("0002"))
    map((3, 300)) should be (Value("0003"))
  }

  it should "allow to read Cassandra table as Array of KV tuples of a case class and a tuple" in {
    val results = sc.cassandraTable[(KeyGroup, (Int, Int, String))]("read_test", "key_value").select("key", "group", "value").collect()
    results should have length 3
    results should contain ((KeyGroup(1, 100), (1, 100, "0001")))
    results should contain ((KeyGroup(2, 100), (2, 100, "0002")))
    results should contain ((KeyGroup(3, 300), (3, 300, "0003")))
  }

  it should "allow to read Cassandra table as Array of tuples of two case classes" in {
    val results = sc.cassandraTable[(KeyGroup, Value)]("read_test", "key_value").select("key", "group", "value").collect()
    results should have length 3
    results should contain((KeyGroup(1, 100), Value("0001")))
    results should contain((KeyGroup(2, 100), Value("0002")))
    results should contain((KeyGroup(3, 300), Value("0003")))
  }

  it should "allow to read Cassandra table as Array of String values" in {
    val results = sc.cassandraTable[String]("read_test", "key_value").select("value").collect()
    results should have length 3
    results should contain("0001")
    results should contain("0002")
    results should contain("0003")
  }

  it should "allow to read Cassandra table as Array of Int values" in {
    val results = sc.cassandraTable[Int]("read_test", "key_value").select("key").collect()
    results should have length 3
    results should contain(1)
    results should contain(2)
    results should contain(3)
  }

  it should "allow to read Cassandra table as Array of java.lang.Integer values" in {
    val results = sc.cassandraTable[Integer]("read_test", "key_value").select("key").collect()
    results should have length 3
    results should contain(1)
    results should contain(2)
    results should contain(3)
  }

  it should "allow to read Cassandra table as Array of List of values" in {
    val results = sc.cassandraTable[List[String]]("read_test", "collections").select("l").collect()
    results should have length 2
    results should contain(List("item1", "item2"))
  }

  it should "allow to read Cassandra table as Array of Set of values" in {
    val results = sc.cassandraTable[Set[String]]("read_test", "collections").select("l").collect()
    results should have length 2
    results should contain(Set("item1", "item2"))
  }

  // This is to trigger result set paging, unused in most other tests:
  it should "allow to count a high number of rows" in {
    val count = sc.cassandraTable("read_test", "big_table").count()
    count should be (bigTableRowCount)
  }
}
