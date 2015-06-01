package com.datastax.spark.connector.rdd

import java.io.IOException
import java.util.Date

import scala.reflect.runtime.universe.typeTag

import org.joda.time.DateTime

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.mapper.DefaultColumnMapper
import com.datastax.spark.connector.types.TypeConverter


case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithConversion(key: String, group: Int, value: Long)
case class CustomerId(id: String)
case class Key(key: Int)
case class KeyGroup(key: Int, group: Int)
case class Value(value: String)
case class WriteTimeClass(id: Int, value: String, writeTimeOfValue: Long)
case class TTLClass(id: Int, value: String, ttlOfValue: Int)
case class ClassWithWeirdProps(devil: Int, cat: Long, value: String)

class MutableKeyValue(var key: Int, var group: Long) extends Serializable {
  var value: String = null
}

class MutableKeyValueWithConversion(var key: String, var group: Int) extends Serializable {
  var value: Long = 0L
}

class SuperKeyValue extends Serializable {
  var key: Int = 0
  var value: String = ""
}

class SubKeyValue extends SuperKeyValue {
  var group: Long = 0L
}

case class Address(street: String, city: String, zip: Int)
case class ClassWithUDT(key: Int, name: String, addr: Address)

class CassandraRDDSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
  val bigTableRowCount = 100000

  private val ks = "CassandraRDDSpec"

  conn.withSessionDo { session =>
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS "$ks" WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }""")

    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))""")
    session.execute(s"""INSERT INTO "$ks".key_value (key, group, value) VALUES (1, 100, '0001')""")
    session.execute(s"""INSERT INTO "$ks".key_value (key, group, value) VALUES (2, 100, '0002')""")
    session.execute(s"""INSERT INTO "$ks".key_value (key, group, value) VALUES (3, 300, '0003')""")

    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".simple_kv (key INT, value TEXT, PRIMARY KEY (key))""")
    session.execute(s"""INSERT INTO "$ks".simple_kv (key, value) VALUES (1, '0001')""")
    session.execute(s"""INSERT INTO "$ks".simple_kv (key, value) VALUES (2, '0002')""")
    session.execute(s"""INSERT INTO "$ks".simple_kv (key, value) VALUES (3, '0003')""")

    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".collections (key INT PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)""")
    session.execute(s"""INSERT INTO "$ks".collections (key, l, s, m) VALUES (1, ['item1', 'item2'], {'item1', 'item2'}, {'key1': 'value1', 'key2': 'value2'})""")
    session.execute(s"""INSERT INTO "$ks".collections (key, l, s, m) VALUES (2, null, null, null)""")

    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".blobs (key INT PRIMARY KEY, b blob)""")
    session.execute(s"""INSERT INTO "$ks".blobs (key, b) VALUES (1, 0x0102030405060708090a0b0c)""")
    session.execute(s"""INSERT INTO "$ks".blobs (key, b) VALUES (2, null)""")

    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".composite_key (key_c1 INT, key_c2 INT, group INT, value TEXT, PRIMARY KEY ((key_c1, key_c2), group))""")
    session.execute(s"""INSERT INTO "$ks".composite_key (key_c1, key_c2, group, value) VALUES (1, 1, 1, 'value1')""")
    session.execute(s"""INSERT INTO "$ks".composite_key (key_c1, key_c2, group, value) VALUES (1, 1, 2, 'value2')""")
    session.execute(s"""INSERT INTO "$ks".composite_key (key_c1, key_c2, group, value) VALUES (1, 2, 3, 'value3')""")
    session.execute(s"""INSERT INTO "$ks".composite_key (key_c1, key_c2, group, value) VALUES (2, 2, 4, 'value4')""")

    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".clustering_time (key INT, time TIMESTAMP, value TEXT, PRIMARY KEY (key, time))""")
    session.execute(s"""INSERT INTO "$ks".clustering_time (key, time, value) VALUES (1, '2014-07-12 20:00:01', 'value1')""")
    session.execute(s"""INSERT INTO "$ks".clustering_time (key, time, value) VALUES (1, '2014-07-12 20:00:02', 'value2')""")
    session.execute(s"""INSERT INTO "$ks".clustering_time (key, time, value) VALUES (1, '2014-07-12 20:00:03', 'value3')""")

    session.execute(s"""CREATE TYPE IF NOT EXISTS "$ks".address (street text, city text, zip int)""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".udts(key INT PRIMARY KEY, name text, addr frozen<address>)""")
    session.execute(s"""INSERT INTO "$ks".udts(key, name, addr) VALUES (1, 'name', {street: 'Some Street', city: 'Paris', zip: 11120})""")

    session.execute("""CREATE KEYSPACE IF NOT EXISTS "MixedSpace" WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }""")
    session.execute("""CREATE TABLE IF NOT EXISTS "MixedSpace"."MixedCase"(key INT PRIMARY KEY, value INT)""")
    session.execute("""CREATE TABLE IF NOT EXISTS "MixedSpace"."MiXEDCase"(key INT PRIMARY KEY, value INT)""")
    session.execute("""CREATE TABLE IF NOT EXISTS "MixedSpace"."MixedCASE"(key INT PRIMARY KEY, value INT)""")
    session.execute("""CREATE TABLE IF NOT EXISTS "MixedSpace"."MoxedCAs" (key INT PRIMARY KEY, value INT)""")

    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".big_table (key INT PRIMARY KEY, value INT)""")
    val insert = session.prepare(s"""INSERT INTO "$ks".big_table(key, value) VALUES (?, ?)""")
    for (i <- 1 to bigTableRowCount) {
      session.execute(insert.bind(i.asInstanceOf[AnyRef], i.asInstanceOf[AnyRef]))
    }

    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".write_time_ttl_test (id INT PRIMARY KEY, value TEXT, value2 TEXT)""")
  }

  "A CassandraRDD" should "allow to read a Cassandra table as Array of CassandraRow" in {
    val result = sc.cassandraTable(ks, "key_value").collect()
    result should have length 3
    result.head.getInt("key") should (be >= 1 and be <= 3)
    result.head.getLong("group") should (be >= 100L and be <= 300L)
    result.head.getString("value") should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of pairs of primitives" in {
    val result = sc.cassandraTable[(Int, Long)](ks, "key_value").select("key", "group").collect()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
  }

  it should "allow to read a Cassandra table as Array of tuples" in {
    val result = sc.cassandraTable[(Int, Long, String)](ks, "key_value").collect()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
    result.head._3 should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class objects" in {
    val result = sc.cassandraTable[KeyValue](ks, "key_value").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.group should (be >= 100L and be <= 300L)
    result.head.value should startWith("000")
  }

  "A CassandraRDD" should "allow to read a Cassandra table as Array of user-defined objects with inherited fields" in {
    val result = sc.cassandraTable[SubKeyValue](ks, "key_value").collect()
    result should have length 3
    result.map(kv => (kv.key, kv.group, kv.value)).toSet shouldBe Set(
      (1, 100, "0001"),
      (2, 100, "0002"),
      (3, 300, "0003")
    )
  }

  it should "allow to read a Cassandra table as Array of user-defined class objects" in {
    val result = sc.cassandraTable[SampleScalaClass](ks, "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined class (with multiple constructors) objects" in {
    val result = sc.cassandraTable[SampleScalaClassWithMultipleCtors](ks, "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined class (with no fields) objects" in {
    val result = sc.cassandraTable[SampleScalaClassWithNoFields](ks, "simple_kv").collect()
    result should have length 3
  }

  it should "allow to read a Cassandra table as Array of user-defined case class (nested) objects" in {
    val result = sc.cassandraTable[SampleWithNestedScalaCaseClass#InnerClass](ks, "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class (deeply nested) objects" in {
    val result = sc.cassandraTable[SampleWithDeeplyNestedScalaCaseClass#IntermediateClass#InnerClass](ks, "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class (nested in object) objects" in {
    val result = sc.cassandraTable[SampleObject.ClassInObject](ks, "simple_kv").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined mutable objects" in {
    val result = sc.cassandraTable[MutableKeyValue](ks, "key_value").collect()
    result should have length 3
    result.head.key should (be >= 1 and be <= 3)
    result.head.group should (be >= 100L and be <= 300L)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table as Array of user-defined case class objects with custom mapping specified by aliases" in {
    val result = sc.cassandraTable[ClassWithWeirdProps](ks, "key_value")
      .select("key" as "devil", "group" as "cat", "value").collect()
    result should have length 3
    result.head.devil should (be >= 1 and be <= 3)
    result.head.cat should (be >= 100L and be <= 300L)
    result.head.value should startWith("000")
  }

  it should "allow to read a Cassandra table into CassandraRow objects with custom mapping specified by aliases" in {
    val result = sc.cassandraTable(ks, "key_value")
      .select("key" as "devil", "group" as "cat", "value").collect()
    result should have length 3
    result.head.getInt("devil") should (be >= 1 and be <= 3)
    result.head.getLong("cat") should (be >= 100L and be <= 300L)
    result.head.getString("value") should startWith("000")
  }

  it should "apply proper data type conversions for tuples" in {
    val result = sc.cassandraTable[(String, Int, Long)](ks, "key_value").collect()
    result should have length 3
    Some(result.head._1) should contain oneOf("1", "2", "3")
    result.head._2 should (be >= 100 and be <= 300)
    result.head._3 should (be >= 1L and be <= 3L)
  }

  it should "apply proper data type conversions for user-defined case class objects" in {
    val result = sc.cassandraTable[KeyValueWithConversion](ks, "key_value").collect()
    result should have length 3
    Some(result.head.key) should contain oneOf("1", "2", "3")
    result.head.group should (be >= 100 and be <= 300)
    result.head.value should (be >= 1L and be <= 3L)
  }

  it should "apply proper data type conversions for user-defined mutable objects" in {
    val result = sc.cassandraTable[MutableKeyValueWithConversion](ks, "key_value").collect()
    result should have length 3
    Some(result.head.key) should contain oneOf("1", "2", "3")
    result.head.group should (be >= 100 and be <= 300)
    result.head.value should (be >= 1L and be <= 3L)
  }

  it should "map columns to objects using user-defined function" in {
    val result = sc.cassandraTable[MutableKeyValue](ks, "key_value")
      .as((key: Int, group: Long, value: String) => (key, group, value)).collect()
    result should have length 3
    result.head._1 should (be >= 1 and be <= 3)
    result.head._2 should (be >= 100L and be <= 300L)
    result.head._3 should startWith("000")
  }

  it should "map columns to objects using user-defined function with type conversion" in {
    val result = sc.cassandraTable[MutableKeyValue](ks, "key_value")
      .as((key: String, group: String, value: Option[String]) => (key, group, value)).collect()
    result should have length 3
    Some(result.head._1) should contain oneOf("1", "2", "3")
    Some(result.head._2) should contain oneOf("100", "300")
    Some(result.head._3) should contain oneOf(Some("0001"), Some("0002"), Some("0003"))
  }

  it should "allow for selecting a subset of columns" in {
    val result = sc.cassandraTable(ks, "key_value").select("value").collect()
    result should have length 3
    result.head.size shouldEqual 1
    result.head.getString("value") should startWith("000")
  }

  it should "allow for selecting a subset of rows" in {
    val result = sc.cassandraTable(ks, "key_value").where("group < ?", 200L).collect()
    result should have length 2
    result.head.size shouldEqual 3
    result.head.getInt("group") shouldEqual 100
    result.head.getString("value") should startWith("000")
  }

  it should "use a single partition for a tiny table" in {
    val rdd = sc.cassandraTable(ks, "key_value")
    rdd.partitions should have length 1
  }

  it should "allow for reading collections" in {
    val result = sc.cassandraTable(ks, "collections").collect()
    val rowById = result.groupBy(_.getInt("key")).mapValues(_.head)
    rowById(1).getList[String]("l") shouldEqual Vector("item1", "item2")
    rowById(1).getSet[String]("s") shouldEqual Set("item1", "item2")
    rowById(1).getMap[String, String]("m") shouldEqual Map("key1" -> "value1", "key2" -> "value2")

    rowById(2).getList[String]("l") shouldEqual Vector.empty
    rowById(2).getSet[String]("s") shouldEqual Set.empty
    rowById(2).getMap[String, String]("m") shouldEqual Map.empty
  }

  it should "allow for reading blobs" in {
    val result = sc.cassandraTable(ks, "blobs").collect()
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

    val result = sc.cassandraTable[(Int, Long, CustomerId)](ks, "key_value").collect()
    result should have length 3
    result(0)._3 shouldNot be(null)
    result(1)._3 shouldNot be(null)
    result(2)._3 shouldNot be(null)
  }

  it should "allow for reading tables with composite partitioning key" in {
    val result = sc.cassandraTable[(Int, Int, Int, String)](ks, "composite_key")
      .where("group >= ?", 3).collect()
    result should have length 2
  }

  it should "convert values passed to where to correct types (String -> Timestamp)" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("time >= ?", "2014-07-12 20:00:02").collect()
    result should have length 2
  }

  it should "convert values passed to where to correct types (DateTime -> Timestamp)" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("time >= ?", new DateTime(2014, 7, 12, 20, 0, 2)).collect()
    result should have length 2
  }

  it should "convert values passed to where to correct types (Date -> Timestamp)" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("time >= ?", new DateTime(2014, 7, 12, 20, 0, 2).toDate).collect()
    result should have length 2
  }

  it should "convert values passed to where to correct types (String -> Timestamp) (double limit)" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("time > ? and time < ?", "2014-07-12 20:00:01", "2014-07-12 20:00:03").collect()
    result should have length 1
  }

  it should "convert values passed to where to correct types (DateTime -> Timestamp) (double limit)" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("time > ? and time < ?", new DateTime(2014, 7, 12, 20, 0, 1), new DateTime(2014, 7, 12, 20, 0, 3)).collect()
    result should have length 1
  }

  it should "convert values passed to where to correct types (Date -> Timestamp) (double limit)" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("time > ? and time < ?", new DateTime(2014, 7, 12, 20, 0, 1).toDate, new DateTime(2014, 7, 12, 20, 0, 3).toDate).collect()
    result should have length 1
  }

  it should "accept partitioning key in where" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("key = ?", 1).collect()
    result should have length 3
  }

  it should "accept partitioning key and clustering column predicate in where" in {
    val result = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("key = ? AND time >= ?", 1, new DateTime(2014, 7, 12, 20, 0, 2).toDate).collect()
    result should have length 2
  }

  it should "accept composite partitioning key in where" in {
    val result = sc.cassandraTable[(Int, Int, Int, String)](ks, "composite_key")
      .where("key_c1 = ? AND key_c2 = ?", 1, 1).collect()
    result should have length 2
  }

  it should "allow to fetch columns from a table with user defined Cassandra type (UDT)" in {
    val result = sc.cassandraTable(ks, "udts").select("key", "name").collect()
    result should have length 1
    val row = result.head
    row.getInt(0) should be(1)
    row.getString(1) should be("name")
  }

  it should "allow to fetch UDT columns as UDTValue objects" in {
    val result = sc.cassandraTable(ks, "udts").select("key", "name", "addr").collect()
    result should have length 1
    val row = result.head
    row.getInt(0) should be(1)
    row.getString(1) should be("name")

    val udtValue = row.getUDTValue(2)
    udtValue.size should be(3)
    udtValue.getString("street") should be("Some Street")
    udtValue.getString("city") should be("Paris")
    udtValue.getInt("zip") should be(11120)
  }

  it should "allow to fetch UDT columns as objects of case classes" in {
    val result = sc.cassandraTable[ClassWithUDT](ks, "udts").select("key", "name", "addr").collect()
    result should have length 1
    val row = result.head
    row.key should be(1)
    row.name should be("name")

    val udtValue = row.addr
    udtValue.street should be("Some Street")
    udtValue.city should be("Paris")
    udtValue.zip should be(11120)
  }

  it should "throw appropriate IOException when the table was not found at the computation time" in {
    intercept[IOException] { sc.cassandraTable(ks, "unknown_table").collect() }
  }

  it should "be lazy and must not throw IOException if the table was not found at the RDD initialization time" in {
    sc.cassandraTable(ks, "unknown_table")
  }

  it should "not leak threads" in {
    // compute a few RDDs so the thread pools get initialized
    // using parallel range, to initialize parallel collections fork-join-pools
    for (i <- (1 to 4).par)
      sc.cassandraTable(ks, "key_value").collect()

    // subsequent computations of RDD should reuse already created thread pools,
    // not instantiate new ones
    val iterationCount = 128
    val startThreadCount = Thread.activeCount()
    for (i <- (1 to iterationCount).par)
      sc.cassandraTable(ks, "key_value").collect()
    val endThreadCount = Thread.activeCount()

    // This is not very precise, but if there was a thread leak and we leaked even only
    // 1-thread per rdd, this test would not pass. Typically we observed the endThreadCount = startThreadCount +/- 3
    // We divide iterationCount here, in order to detect thread leaks that would not happen every time and to account
    // for a few threads exiting during the test.
    endThreadCount should be < startThreadCount + iterationCount / 2
  }

  it should "allow to read Cassandra table as Array of KV tuples of two pairs" in {
    val results = sc.cassandraTable[((Int, Int), (Int, String))](ks, "composite_key").select("key_c1", "key_c2" ,"group", "value").collect()
    results should have length 4
    results should contain (((1, 1), (1, "value1")))
    results should contain (((1, 1), (2, "value2")))
    results should contain (((1, 2), (3, "value3")))
    results should contain (((2, 2), (4, "value4")))
  }

  it should "allow to read Cassandra table as Array of KV tuples of a pair and a case class" in {
    val results = sc.cassandraTable[((Int, Int), Value)](ks, "key_value").select("key", "group", "value").collect()
    results should have length 3
    val map = results.toMap
    map((1, 100)) should be (Value("0001"))
    map((2, 100)) should be (Value("0002"))
    map((3, 300)) should be (Value("0003"))
  }

  it should "allow to read Cassandra table as Array of KV tuples of a case class and a tuple" in {
    val results = sc.cassandraTable[(KeyGroup, (Int, Int, String))](ks, "key_value").select("key", "group", "value").collect()
    results should have length 3
    results should contain ((KeyGroup(1, 100), (1, 100, "0001")))
    results should contain ((KeyGroup(2, 100), (2, 100, "0002")))
    results should contain ((KeyGroup(3, 300), (3, 300, "0003")))
  }

  it should "allow to read Cassandra table as Array of KV tuples of a case class and a tuple grouped by partition key" in {

    conn.withSessionDo { session =>
      session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".wide_rows(key INT, group INT, value VARCHAR, PRIMARY KEY (key, group))""")
      session.execute(s"""INSERT INTO "$ks".wide_rows(key, group, value) VALUES (10, 10, '1010')""")
      session.execute(s"""INSERT INTO "$ks".wide_rows(key, group, value) VALUES (10, 11, '1011')""")
      session.execute(s"""INSERT INTO "$ks".wide_rows(key, group, value) VALUES (10, 12, '1012')""")
      session.execute(s"""INSERT INTO "$ks".wide_rows(key, group, value) VALUES (20, 20, '2020')""")
      session.execute(s"""INSERT INTO "$ks".wide_rows(key, group, value) VALUES (20, 21, '2021')""")
      session.execute(s"""INSERT INTO "$ks".wide_rows(key, group, value) VALUES (20, 22, '2022')""")
    }

    val results = sc
      .cassandraTable[(Key, (Int, Int, String))](ks, "wide_rows")
      .select("key", "group", "value")
      .spanByKey
      .collect()
      .toMap

    results should have size 2
    results should contain key Key(10)
    results should contain key Key(20)

    results(Key(10)) should contain inOrder(
      (10, 10, "1010"),
      (10, 11, "1011"),
      (10, 12, "1012"))

    results(Key(20)) should contain inOrder(
      (20, 20, "2020"),
      (20, 21, "2021"),
      (20, 22, "2022"))
  }


  it should "allow to read Cassandra table as Array of tuples of two case classes" in {
    val results = sc.cassandraTable[(KeyGroup, Value)](ks, "key_value").select("key", "group", "value").collect()
    results should have length 3
    results should contain((KeyGroup(1, 100), Value("0001")))
    results should contain((KeyGroup(2, 100), Value("0002")))
    results should contain((KeyGroup(3, 300), Value("0003")))
  }

  it should "allow to read Cassandra table as Array of String values" in {
    val results = sc.cassandraTable[String](ks, "key_value").select("value").collect()
    results should have length 3
    results should contain("0001")
    results should contain("0002")
    results should contain("0003")
  }

  it should "allow to read Cassandra table as Array of Int values" in {
    val results = sc.cassandraTable[Int](ks, "key_value").select("key").collect()
    results should have length 3
    results should contain(1)
    results should contain(2)
    results should contain(3)
  }

  it should "allow to read Cassandra table as Array of java.lang.Integer values" in {
    val results = sc.cassandraTable[Integer](ks, "key_value").select("key").collect()
    results should have length 3
    results should contain(1)
    results should contain(2)
    results should contain(3)
  }

  it should "allow to read Cassandra table as Array of List of values" in {
    val results = sc.cassandraTable[List[String]](ks, "collections").select("l").collect()
    results should have length 2
    results should contain(List("item1", "item2"))
  }

  it should "allow to read Cassandra table as Array of Set of values" in {
    val results = sc.cassandraTable[Set[String]](ks, "collections").select("l").collect()
    results should have length 2
    results should contain(Set("item1", "item2"))
  }

  // This is to trigger result set paging, unused in most other tests:
  it should "allow to count a high number of rows" in {
    val count = sc.cassandraTable(ks, "big_table").count()
    count should be (bigTableRowCount)
  }

  it should "allow to fetch write time of a specified column as a tuple element" in {
    val writeTime = System.currentTimeMillis() * 1000L
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE "$ks".write_time_ttl_test""")
      session.execute(s"""INSERT INTO "$ks".write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TIMESTAMP $writeTime""")
    }
    val results = sc.cassandraTable[(Int, String, Long)](ks, "write_time_ttl_test")
      .select("id", "value", "value".writeTime).collect().headOption
    results.isDefined should be(true)
    results.get should be((1, "test", writeTime))
  }

  it should "allow to fetch ttl of a specified column as a tuple element" in {
    val ttl = 1000
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE "$ks".write_time_ttl_test""")
      session.execute(s"""INSERT INTO "$ks".write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TTL $ttl""")
    }
    val results = sc.cassandraTable[(Int, String, Int)](ks, "write_time_ttl_test")
      .select("id", "value", "value".ttl).collect().headOption
    results.isDefined should be(true)
    results.get._1 should be (1)
    results.get._2 should be ("test")
    results.get._3 should be > (ttl - 10)
    results.get._3 should be <= ttl
  }

  it should "allow to fetch both write time and ttl of a specified column as tuple elements" in {
    val writeTime = System.currentTimeMillis() * 1000L
    val ttl = 1000
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE "$ks".write_time_ttl_test""")
      session.execute(s"""INSERT INTO "$ks".write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TIMESTAMP $writeTime AND TTL $ttl""")
    }
    val results = sc.cassandraTable[(Int, String, Long, Int)](ks, "write_time_ttl_test")
      .select("id", "value", "value".writeTime, "value".ttl).collect().headOption
    results.isDefined should be(true)
    results.get._1 should be (1)
    results.get._2 should be ("test")
    results.get._3 should be (writeTime)
    results.get._4 should be > (ttl - 10)
    results.get._4 should be <= ttl
  }

  it should "allow to fetch write time of two different columns as tuple elements" in {
    val writeTime = System.currentTimeMillis() * 1000L
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE "$ks".write_time_ttl_test""")
      session.execute(s"""INSERT INTO "$ks".write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TIMESTAMP $writeTime""")
    }
    val results = sc.cassandraTable[(Int, Long, Long)](ks, "write_time_ttl_test")
      .select("id", "value".writeTime, "value2".writeTime).collect().headOption
    results.isDefined should be(true)
    results.get should be((1, writeTime, writeTime))
  }

  it should "allow to fetch ttl of two different columns as tuple elements" in {
    val ttl = 1000
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE "$ks".write_time_ttl_test""")
      session.execute(s"""INSERT INTO "$ks".write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TTL $ttl""")
    }
    val results = sc.cassandraTable[(Int, Int, Int)](ks, "write_time_ttl_test")
      .select("id", "value".ttl, "value2".ttl).collect().headOption
    results.isDefined should be(true)
    results.get._1 should be (1)
    results.get._2 should be > (ttl - 10)
    results.get._2 should be <= ttl
    results.get._3 should be > (ttl - 10)
    results.get._3 should be <= ttl
  }

  it should "allow to fetch writetime of a specified column and map it to a class field with custom mapping" in {
    val writeTime = System.currentTimeMillis() * 1000L
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE "$ks".write_time_ttl_test""")
      session.execute(s"""INSERT INTO "$ks".write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TIMESTAMP $writeTime""")
    }
    implicit val mapper = new DefaultColumnMapper[WriteTimeClass](Map("writeTimeOfValue" -> "value".writeTime.selectedAs))
    val results = sc.cassandraTable[WriteTimeClass](ks, "write_time_ttl_test")
      .select("id", "value", "value".writeTime).collect().headOption
    results.isDefined should be (true)
    results.head should be (WriteTimeClass(1, "test", writeTime))
  }

  it should "allow to fetch ttl of a specified column and map it to a class field with custom mapping" in {
    val ttl = 1000
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE "$ks".write_time_ttl_test""")
      session.execute(s"""INSERT INTO "$ks".write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TTL $ttl""")
    }
    implicit val mapper = new DefaultColumnMapper[TTLClass](Map("ttlOfValue" -> "value".ttl.selectedAs))
    val results = sc.cassandraTable[TTLClass](ks, "write_time_ttl_test")
      .select("id", "value", "value".ttl).collect().headOption
    results.isDefined should be (true)
    results.head.id should be (1)
    results.head.value should be ("test")
    results.head.ttlOfValue > (ttl - 10)
    results.head.ttlOfValue <= ttl
  }

  it should "allow to fetch writetime of a specified column and map it to a class field with aliases" in {
    val writeTime = System.currentTimeMillis() * 1000L
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE "$ks".write_time_ttl_test""")
      session.execute(s"""INSERT INTO "$ks".write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TIMESTAMP $writeTime""")
    }
    val results = sc.cassandraTable[WriteTimeClass](ks, "write_time_ttl_test")
      .select("id", "value", "value".writeTime as "writeTimeOfValue").collect().headOption
    results.isDefined should be (true)
    results.head should be (WriteTimeClass(1, "test", writeTime))
  }

  it should "allow to fetch ttl of a specified column and map it to a class field with aliases" in {
    val ttl = 1000
    conn.withSessionDo { session =>
      session.execute(s"""TRUNCATE "$ks".write_time_ttl_test""")
      session.execute(s"""INSERT INTO "$ks".write_time_ttl_test (id, value, value2) VALUES (1, 'test', 'test2') USING TTL $ttl""")
    }
    val results = sc.cassandraTable[TTLClass](ks, "write_time_ttl_test")
      .select("id", "value", "value".ttl as "ttlOfValue").collect().headOption
    results.isDefined should be (true)
    results.head.id should be (1)
    results.head.value should be ("test")
    results.head.ttlOfValue > (ttl - 10)
    results.head.ttlOfValue <= ttl
  }

  it should "allow to specify ascending ordering" in {
    val results = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("key=1").withAscOrder.collect()
    results.map(_._3).toList shouldBe List("value1", "value2", "value3")
  }

  it should "allow to specify descending ordering" in {
    val results = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time")
      .where("key=1").withDescOrder.collect()
    results.map(_._3).toList shouldBe List("value3", "value2", "value1")
  }

  it should "allow to specify rows number limit" in {
    val results = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time").where("key=1").limit(2).collect()
    results should have length 2
    results(0)._3 shouldBe "value1"
    results(1)._3 shouldBe "value2"
  }

  it should "allow to specify rows number with take" in {
    val results = sc.cassandraTable[(Int, Date, String)](ks, "clustering_time").where("key=1").take(2)
    results should have length 2
    results(0)._3 shouldBe "value1"
    results(1)._3 shouldBe "value2"
  }

  it should "count the CassandraRDD items" in {
    val result = sc.cassandraTable(ks, "big_table").count()
    result shouldBe bigTableRowCount
  }

  it should "count the CassandraRDD items with where predicate" in {
    val result = sc.cassandraTable(ks, "big_table").where("key=1").count()
    result shouldBe 1
  }

  it should "allow to use empty RDD on undefined table" in {
    val result = sc.cassandraTable("unknown_ks", "unknown_table").toEmptyCassandraRDD.collect()
    result should have length 0
  }

  it should "allow to use empty RDD on defined table" in {
    val result = sc.cassandraTable(ks, "simple_kv").toEmptyCassandraRDD.collect()
    result should have length 0
  }

  it should "suggest similar tables if table doesn't exist but keyspace does" in {
    val ioe = the [IOException] thrownBy sc.cassandraTable("MixedSpace","mixedcase").collect()
    val message = ioe.getMessage
    message should include ("MixedSpace.MixedCase")
    message should include ("MixedSpace.MiXEDCase")
    message should include ("MixedSpace.MixedCASE")
  }

  it should "suggest possible keyspace and table matches if the keyspace and table do not exist" in {
    val ioe = the [IOException] thrownBy sc.cassandraTable("MoxedSpace","mixdcase").collect()
    val message = ioe.getMessage
    message should include ("MixedSpace.MixedCase")
    message should include ("MixedSpace.MiXEDCase")
    message should include ("MixedSpace.MixedCASE")
  }

  it should "suggest possible keyspaces if the table exists but in a different keyspace" in {
    val ioe = the [IOException] thrownBy sc.cassandraTable("MoxedSpace","MoxedCAS").collect()
    val message = ioe.getMessage
    message should include ("MixedSpace.MoxedCAs")
  }

  it should "suggest possible keyspaces and tables if the table has a fuzzy match but they keyspace does not" in {
    val ioe = the [IOException] thrownBy sc.cassandraTable("rock","MixedCase").collect()
    val message = ioe.getMessage
    message should include ("MixedSpace.MixedCase")
  }

}
