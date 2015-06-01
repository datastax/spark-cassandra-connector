package com.datastax.spark.connector.writer

import java.io.IOException

import scala.reflect.ClassTag

import com.datastax.spark.connector.mapper.{ColumnMapper, DefaultColumnMapper}

import scala.collection.JavaConversions._

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.types.{BigIntType, TextType, IntType, TypeConverter}
import com.datastax.spark.connector.embedded._

case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithTransient(key: Int, group: Long, value: String, @transient transientField: String)
case class KeyValueWithTTL(key: Int, group: Long, value: String, ttl: Int)
case class KeyValueWithTimestamp(key: Int, group: Long, value: String, timestamp: Long)
case class KeyValueWithConversion(key: String, group: Int, value: String)
case class ClassWithWeirdProps(devil: String, cat: Int, value: String)

class SuperKeyValue(val key: Int, val value: String) extends Serializable

class SubKeyValue(k: Int, v: String, val group: Long) extends SuperKeyValue(k, v)

case class CustomerId(id: String)
object CustomerIdConverter extends TypeConverter[String] {
  def targetTypeTag = scala.reflect.runtime.universe.typeTag[String]
  def convertPF = { case CustomerId(id) => id }
}

class TableWriterSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  val ks = "TableWriterSpec"
  
  conn.withSessionDo { session =>
    session.execute(s"""DROP KEYSPACE IF EXISTS "$ks"""")
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS "$ks" WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }""")

    for (x <- 1 to 19) {
      session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".key_value_$x (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))""")
    }

    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".nulls (key INT PRIMARY KEY, text_value TEXT, int_value INT)""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".collections (key INT PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".blobs (key INT PRIMARY KEY, b blob)""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".counters (pkey INT, ckey INT, c1 counter, c2 counter, PRIMARY KEY (pkey, ckey))""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".counters2 (pkey INT PRIMARY KEY, c counter)""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".\"camelCase\" (\"primaryKey\" INT PRIMARY KEY, \"textValue\" text)""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".single_column (pk INT PRIMARY KEY)""")

    session.execute(s"""CREATE TYPE "$ks".address (street text, city text, zip int)""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".udts(key INT PRIMARY KEY, name text, addr frozen<address>)""")

  }

  private def verifyKeyValueTable(tableName: String) {
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".""" + tableName).all()
      result should have size 3
      for (row <- result) {
        Some(row.getInt(0)) should contain oneOf(1, 2, 3)
        Some(row.getLong(1)) should contain oneOf(1, 2, 3)
        Some(row.getString(2)) should contain oneOf("value1", "value2", "value3")
      }
    }
  }

  "A TableWriter" should "write RDD of tuples to an existing table" in {
    val col = Seq((1, 1L, "value1"), (2, 2L, "value2"), (3, 3L, "value3"))
    sc.parallelize(col).saveToCassandra(ks, "key_value_1", SomeColumns("key", "group", "value"))
    verifyKeyValueTable("key_value_1")
  }

  it should "write RDD of tuples to a new table" in {
    val pkey = ColumnDef("key", PartitionKeyColumn, IntType)
    val group = ColumnDef("group", ClusteringColumn(0), BigIntType)
    val value = ColumnDef("value", RegularColumn, TextType)
    val table = TableDef(ks, "new_kv_table", Seq(pkey), Seq(group), Seq(value))
    val rows = Seq((1, 1L, "value1"), (2, 2L, "value2"), (3, 3L, "value3"))
    sc.parallelize(rows).saveAsCassandraTableEx(table, SomeColumns("key", "group", "value"))
    verifyKeyValueTable("new_kv_table")
  }

  it should "write RDD of tuples applying proper data type conversions" in {
    val col = Seq(("1", "1", "value1"), ("2", "2", "value2"), ("3", "3", "value3"))
    sc.parallelize(col).saveToCassandra(ks, "key_value_2")
    verifyKeyValueTable("key_value_2")
  }

  it should "write RDD of case class objects" in {
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    sc.parallelize(col).saveToCassandra(ks, "key_value_3")
    verifyKeyValueTable("key_value_3")
  }

  it should "write RDD of case class objects to a new table using auto mapping" in {
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    sc.parallelize(col).saveAsCassandraTable(ks, "new_kv_table_from_case_class")
    verifyKeyValueTable("new_kv_table_from_case_class")
  }

  it should "write RDD of case class objects applying proper data type conversions" in {
    val col = Seq(
      KeyValueWithConversion("1", 1, "value1"),
      KeyValueWithConversion("2", 2, "value2"),
      KeyValueWithConversion("3", 3, "value3")
    )
    sc.parallelize(col).saveToCassandra(ks, "key_value_4")
    verifyKeyValueTable("key_value_4")
  }

  it should "write RDD of CassandraRow objects" in {
    val col = Seq(
      CassandraRow.fromMap(Map("key" -> 1, "group" -> 1L, "value" -> "value1")),
      CassandraRow.fromMap(Map("key" -> 2, "group" -> 2L, "value" -> "value2")),
      CassandraRow.fromMap(Map("key" -> 3, "group" -> 3L, "value" -> "value3"))
    )
    sc.parallelize(col).saveToCassandra(ks, "key_value_5")
    verifyKeyValueTable("key_value_5")
  }

  it should "write RDD of CassandraRow objects applying proper data type conversions" in {
    val col = Seq(
      CassandraRow.fromMap(Map("key" -> "1", "group" -> BigInt(1), "value" -> "value1")),
      CassandraRow.fromMap(Map("key" -> "2", "group" -> BigInt(2), "value" -> "value2")),
      CassandraRow.fromMap(Map("key" -> "3", "group" -> BigInt(3), "value" -> "value3"))
    )
    sc.parallelize(col).saveToCassandra(ks, "key_value_6")
    verifyKeyValueTable("key_value_6")
  }

  it should "write RDD of tuples to a table with camel case column names" in {
    val col = Seq((1, "value1"), (2, "value2"), (3, "value3"))
    sc.parallelize(col).saveToCassandra(ks, "camelCase", SomeColumns("primaryKey", "textValue"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks"."camelCase"""").all()
      result should have size 3
      for (row <- result) {
        Some(row.getInt(0)) should contain oneOf(1, 2, 3)
        Some(row.getString(1)) should contain oneOf("value1", "value2", "value3")
      }
    }
  }

  it should "write empty values" in {
    val col = Seq((1, 1L, None))
    sc.parallelize(col).saveToCassandra(ks, "key_value_7", SomeColumns("key", "group", "value"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".key_value_7""").all()
      result should have size 1
      for (row <- result) {
        row.getString(2) should be (null)
      }
    }
  }

  it should "write null values" in {
    val key = 1.asInstanceOf[AnyRef]
    val row = new CassandraRow(IndexedSeq("key", "text_value", "int_value"), IndexedSeq(key, null, null))

    sc.parallelize(Seq(row)).saveToCassandra(ks, "nulls")
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".nulls""").all()
      result should have size 1
      for (r <- result) {
        r.getInt(0) shouldBe key
        r.isNull(1) shouldBe true
        r.isNull(2) shouldBe true
      }
    }
  }

  it should "write only specific column data if ColumnNames is passed as 'columnNames'" in {
    val col = Seq((1, 1L, None))
    sc.parallelize(col).saveToCassandra(ks, "key_value_8", SomeColumns("key", "group"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".key_value_8""").all()
      result should have size 1
      for (row <- result) {
        row.getInt(0) should be (1)
        row.getString(2) should be (null)
      }
    }
  }

  it should "distinguish (deprecated) implicit `seqToSomeColumns`" in {
    val col = Seq((2, 1L, None))
    sc.parallelize(col).saveToCassandra(ks, "key_value_9", SomeColumns("key", "group"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".key_value_9""").all()
      result should have size 1
      for (row <- result) {
        row.getInt(0) should be (2)
        row.getString(2) should be (null)
      }
    }
  }

  it should "write collections" in {
    val col = Seq(
      (1, Vector("item1", "item2"), Set("item1", "item2"), Map("key1" -> "value1", "key2" -> "value2")),
      (2, Vector.empty[String], Set.empty[String], Map.empty[String, String]))
    sc.parallelize(col).saveToCassandra(ks, "collections", SomeColumns("key", "l", "s", "m"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".collections""").all()
      result should have size 2
      val rows = result.groupBy(_.getInt(0)).mapValues(_.head)
      val row0 = rows(1)
      val row1 = rows(2)
      row0.getList("l", classOf[String]).toSeq shouldEqual Seq("item1", "item2")
      row0.getSet("s", classOf[String]).toSeq shouldEqual Seq("item1", "item2")
      row0.getMap("m", classOf[String], classOf[String]).toMap shouldEqual Map("key1" -> "value1", "key2" -> "value2")
      row1.isNull("l") shouldEqual true
      row1.isNull("m") shouldEqual true
      row1.isNull("s") shouldEqual true
    }
  }

  it should "write blobs" in {
    val col = Seq((1, Some(Array[Byte](0, 1, 2, 3))), (2, None))
    sc.parallelize(col).saveToCassandra(ks, "blobs", SomeColumns("key", "b"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".blobs""").all()
      result should have size 2
      val rows = result.groupBy(_.getInt(0)).mapValues(_.head)
      val row0 = rows(1)
      val row1 = rows(2)
      row0.getBytes("b").remaining shouldEqual 4
      row1.isNull("b") shouldEqual true
    }
  }

  it should "increment and decrement counters" in {
    val col1 = Seq((0, 0, 1, 1))
    sc.parallelize(col1).saveToCassandra(ks, "counters", SomeColumns("pkey", "ckey", "c1", "c2"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".counters""").one()
      result.getLong("c1") shouldEqual 1L
      result.getLong("c2") shouldEqual 1L
    }
    val col2 = Seq((0, 0, 1))
    sc.parallelize(col1).saveToCassandra(ks, "counters", SomeColumns("pkey", "ckey", "c2"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".counters""").one()
      result.getLong("c1") shouldEqual 1L
      result.getLong("c2") shouldEqual 2L
    }
  }

  it should "increment and decrement counters in batches" in {
    val rowCount = 10000
    val col = for (i <- 1 to rowCount) yield (i, 1)
    sc.parallelize(col).saveToCassandra(ks, "counters2", SomeColumns("pkey", "c"))
    sc.cassandraTable(ks, "counters2").count should be(rowCount)
  }

  it should "write values of user-defined classes" in {
    TypeConverter.registerConverter(CustomerIdConverter)

    val col = Seq((1, 1L, CustomerId("foo")))
    sc.parallelize(col).saveToCassandra(ks, "key_value_10", SomeColumns("key", "group", "value"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".key_value_10""").all()
      result should have size 1
      for (row <- result)
        row.getString(2) shouldEqual "foo"
    }
  }

  it should "write values of user-defined-types in Cassandra" in {
    val address = UDTValue.fromMap(Map("city" -> "Warsaw", "zip" -> 10000, "street" -> "Marszałkowska"))
    val col = Seq((1, "Joe", address))
    sc.parallelize(col).saveToCassandra(ks, "udts", SomeColumns("key", "name", "addr"))

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, name, addr FROM "$ks".udts""").all()
      result should have size 1
      for (row <- result) {
        row.getInt(0) shouldEqual 1
        row.getString(1) shouldEqual "Joe"
        row.getUDTValue(2).getString("city") shouldEqual "Warsaw"
        row.getUDTValue(2).getInt("zip") shouldEqual 10000
      }
    }
  }


  it should "write to single-column tables" in {
    val col = Seq(1, 2, 3, 4, 5).map(Tuple1.apply)
    sc.parallelize(col).saveToCassandra(ks, "single_column", SomeColumns("pk"))
    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT * FROM "$ks".single_column""").all()
      result should have size 5
      result.map(_.getInt(0)).toSet should be (Set(1, 2, 3, 4, 5))
    }
  }

  it should "throw IOException if table is not found" in {
    val col = Seq(("1", "1", "value1"), ("2", "2", "value2"), ("3", "3", "value3"))
    intercept[IOException] {
      sc.parallelize(col).saveToCassandra(ks, "unknown_table")
    }
  }

  it should "write RDD of case class objects with default TTL" in {
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    sc.parallelize(col).saveToCassandra(ks, "key_value_11", writeConf = WriteConf(ttl = TTLOption.constant(100)))

    verifyKeyValueTable("key_value_11")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT TTL(value) FROM "$ks".key_value_11""").all()
      result should have size 3
      result.foreach(_.getInt(0) should be > 50)
      result.foreach(_.getInt(0) should be <= 100)
    }
  }

  it should "write RDD of case class objects with default timestamp" in {
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    val ts = System.currentTimeMillis() - 1000L
    sc.parallelize(col).saveToCassandra(ks, "key_value_12", writeConf = WriteConf(timestamp = TimestampOption.constant(ts * 1000L)))

    verifyKeyValueTable("key_value_12")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT WRITETIME(value) FROM "$ks".key_value_12""").all()
      result should have size 3
      result.foreach(_.getLong(0) should be (ts * 1000L))
    }
  }

  it should "write RDD of case class objects with per-row TTL" in {
    val col = Seq(KeyValueWithTTL(1, 1L, "value1", 100), KeyValueWithTTL(2, 2L, "value2", 200), KeyValueWithTTL(3, 3L, "value3", 300))
    sc.parallelize(col).saveToCassandra(ks, "key_value_13", writeConf = WriteConf(ttl = TTLOption.perRow("ttl")))

    verifyKeyValueTable("key_value_13")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, TTL(value) FROM "$ks".key_value_13""").all()
      result should have size 3
      result.foreach(row => {
        row.getInt(1) should be > (100 * row.getInt(0) - 50)
        row.getInt(1) should be <= (100 * row.getInt(0))
      })
    }
  }

  it should "write RDD of case class objects with per-row timestamp" in {
    val ts = System.currentTimeMillis() - 1000L
    val col = Seq(KeyValueWithTimestamp(1, 1L, "value1", ts * 1000L + 100L), KeyValueWithTimestamp(2, 2L, "value2", ts * 1000L + 200L), KeyValueWithTimestamp(3, 3L, "value3", ts * 1000L + 300L))
    sc.parallelize(col).saveToCassandra(ks, "key_value_14", writeConf = WriteConf(timestamp = TimestampOption.perRow("timestamp")))

    verifyKeyValueTable("key_value_14")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, WRITETIME(value) FROM "$ks".key_value_14""").all()
      result should have size 3
      result.foreach(row => {
        row.getLong(1) should be (ts * 1000L + row.getInt(0) * 100L)
      })
    }
  }

  it should "write RDD of case class objects with per-row TTL with custom mapping" in {
    val col = Seq(KeyValueWithTTL(1, 1L, "value1", 100), KeyValueWithTTL(2, 2L, "value2", 200), KeyValueWithTTL(3, 3L, "value3", 300))
    implicit val mapping = new DefaultColumnMapper[KeyValueWithTTL](Map("ttl" -> "ttl_placeholder"))

    sc.parallelize(col).saveToCassandra(ks, "key_value_15",
      writeConf = WriteConf(ttl = TTLOption.perRow("ttl_placeholder")))

    verifyKeyValueTable("key_value_15")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, TTL(value) FROM "$ks".key_value_15""").all()
      result should have size 3
      result.foreach(row => {
        row.getInt(1) should be > (100 * row.getInt(0) - 50)
        row.getInt(1) should be <= (100 * row.getInt(0))
      })
    }
  }

  it should "write RDD of case class objects with per-row timestamp with custom mapping" in {
    val ts = System.currentTimeMillis() - 1000L
    val col = Seq(KeyValueWithTimestamp(1, 1L, "value1", ts * 1000L + 100L), KeyValueWithTimestamp(2, 2L, "value2", ts * 1000L + 200L), KeyValueWithTimestamp(3, 3L, "value3", ts * 1000L + 300L))

    implicit val mapper =
      new DefaultColumnMapper[KeyValueWithTimestamp](Map("timestamp" -> "timestamp_placeholder"))

    sc.parallelize(col).saveToCassandra(ks, "key_value_16",
      writeConf = WriteConf(timestamp = TimestampOption.perRow("timestamp_placeholder")))

    verifyKeyValueTable("key_value_16")

    conn.withSessionDo { session =>
      val result = session.execute(s"""SELECT key, WRITETIME(value) FROM "$ks".key_value_16""").all()
      result should have size 3
      result.foreach(row => {
        row.getLong(1) should be (ts * 1000L + row.getInt(0) * 100L)
      })
    }
  }

  it should "write RDD of case class objects applying proper data type conversions and aliases" in {
    val col = Seq(
      ClassWithWeirdProps("1", 1, "value1"),
      ClassWithWeirdProps("2", 2, "value2"),
      ClassWithWeirdProps("3", 3, "value3")
    )
    sc.parallelize(col).saveToCassandra(ks, "key_value_17", columns = SomeColumns(
      "key" as "devil", "group" as "cat", "value"
    ))
    verifyKeyValueTable("key_value_17")
  }

  it should "write RDD of objects with inherited fields" in {
    val col = Seq(
      new SubKeyValue(1, "value1", 1L),
      new SubKeyValue(2, "value2", 2L),
      new SubKeyValue(3, "value3", 3L)
    )
    sc.parallelize(col).saveToCassandra(ks, "key_value_18")
    verifyKeyValueTable("key_value_18")
  }

  it should "write RDD of case class objects with transient fields" in {
    val col = Seq(KeyValueWithTransient(1, 1L, "value1", "a"), KeyValueWithTransient(2, 2L, "value2", "b"), KeyValueWithTransient(3, 3L, "value3", "c"))
    sc.parallelize(col).saveToCassandra(ks, "key_value_19")
    verifyKeyValueTable("key_value_19")
  }

}
