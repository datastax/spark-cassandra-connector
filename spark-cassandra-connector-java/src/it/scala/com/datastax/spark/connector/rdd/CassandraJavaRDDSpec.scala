package com.datastax.spark.connector.rdd

import java.io.IOException

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.japi.CassandraJavaUtil
import com.datastax.spark.connector.mapper.NamedColumnRef
import com.datastax.spark.connector.testkit._
import com.datastax.spark.connector.types.TypeConverter
import org.apache.commons.lang3.tuple
import org.scalatest._

import scala.collection.JavaConversions._
import CassandraJavaUtil._

class CassandraJavaRDDSpec extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll
with ShouldMatchers with SharedEmbeddedCassandra with SparkTemplate {

  useCassandraConfig("cassandra-default.yaml.template")

  val conn = CassandraConnector(Set(EmbeddedCassandra.cassandraHost))

  conn.withSessionDo { session =>
    session.execute("DROP KEYSPACE IF EXISTS java_api_test")
    session.execute("CREATE KEYSPACE java_api_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE java_api_test.test_table (key INT, value TEXT, PRIMARY KEY (key))")
    session.execute("CREATE INDEX test_table_idx ON java_api_test.test_table (value)")
    session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (1, 'one')")
    session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (2, 'two')")
    session.execute("INSERT INTO java_api_test.test_table (key, value) VALUES (3,  null)")

    session.execute("CREATE TABLE java_api_test.test_table2 (some_key INT, some_value TEXT, PRIMARY KEY (some_key))")
    session.execute("INSERT INTO java_api_test.test_table2 (some_key, some_value) VALUES (1, 'one')")
    session.execute("INSERT INTO java_api_test.test_table2 (some_key, some_value) VALUES (2, 'two')")
    session.execute("INSERT INTO java_api_test.test_table2 (some_key, some_value) VALUES (3, null)")

    session.execute("CREATE TABLE IF NOT EXISTS java_api_test.collections (key INT PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)")
    session.execute("INSERT INTO java_api_test.collections (key, l, s, m) VALUES (1, ['item1', 'item2'], {'item1', 'item2'}, {'key1': 'value1', 'key2': 'value2'})")
    session.execute("INSERT INTO java_api_test.collections (key, l, s, m) VALUES (2, null, null, null)")

    session.execute("CREATE TABLE IF NOT EXISTS java_api_test.nulls (key INT PRIMARY KEY, i int, vi varint, t text, d timestamp, l list<int>)")
    session.execute("INSERT INTO java_api_test.nulls (key, i, vi, t, d, l) VALUES (1, null, null, null, null, null)")

  }

  "CassandraJavaRDD" should "allow to read data as CassandraRows " in {
    val rows = javaFunctions(sc).cassandraTable("java_api_test", "test_table").collect()
    assert(rows.size == 3)
    assert(rows.exists(row ⇒ row.getString("value") == "one" && row.getInt("key") == 1))
    assert(rows.exists(row ⇒ row.getString("value") == "two" && row.getInt("key") == 2))
    assert(rows.exists(row ⇒ row.getString("value") == null && row.getInt("key") == 3))
  }

  it should "allow to read data as Java beans " in {
    val beans = javaFunctions(sc).cassandraTable("java_api_test", "test_table", mapRowTo(classOf[SampleJavaBean])).collect()
    assert(beans.size == 3)
    assert(beans.exists(bean ⇒ bean.getValue == "one" && bean.getKey == 1))
    assert(beans.exists(bean ⇒ bean.getValue == "two" && bean.getKey == 2))
    assert(beans.exists(bean ⇒ bean.getValue == null && bean.getKey == 3))
  }

  it should "allow to read data as Java beans (with multiple constructors)" in {
    val beans = javaFunctions(sc).cassandraTable("java_api_test", "test_table", mapRowTo(classOf[SampleJavaBeanWithMultipleCtors])).collect()
    assert(beans.size == 3)
    assert(beans.exists(bean ⇒ bean.getValue == "one" && bean.getKey == 1))
    assert(beans.exists(bean ⇒ bean.getValue == "two" && bean.getKey == 2))
    assert(beans.exists(bean ⇒ bean.getValue == null && bean.getKey == 3))
  }

  it should "throw NoSuchMethodException when trying to read data as Java beans (without no-args constructor)" in {
    intercept[NoSuchMethodException](javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table", mapRowTo(classOf[SampleJavaBeanWithoutNoArgsCtor])).collect())
  }

  it should "allow to read data as nested Java beans" in {
    val beans = javaFunctions(sc).cassandraTable("java_api_test", "test_table", mapRowTo(classOf[SampleWithNestedJavaBean#InnerClass])).collect()
    assert(beans.size == 3)
    assert(beans.exists(bean ⇒ bean.getValue == "one" && bean.getKey == 1))
    assert(beans.exists(bean ⇒ bean.getValue == "two" && bean.getKey == 2))
    assert(beans.exists(bean ⇒ bean.getValue == null && bean.getKey == 3))
  }

  it should "allow to read data as deeply nested Java beans" in {
    val beans = javaFunctions(sc).cassandraTable("java_api_test", "test_table",
      mapRowTo(classOf[SampleWithDeeplyNestedJavaBean#IntermediateClass#InnerClass])).collect()
    assert(beans.size == 3)
    assert(beans.exists(bean ⇒ bean.getValue == "one" && bean.getKey == 1))
    assert(beans.exists(bean ⇒ bean.getValue == "two" && bean.getKey == 2))
    assert(beans.exists(bean ⇒ bean.getValue == null && bean.getKey == 3))
  }


  it should "allow to select a subset of columns" in {
    val rows = javaFunctions(sc).cassandraTable("java_api_test", "test_table")
      .select("key").collect()
    assert(rows.size == 3)
    assert(rows.exists(row ⇒ !row.contains("value") && row.getInt("key") == 1))
    assert(rows.exists(row ⇒ !row.contains("value") && row.getInt("key") == 2))
    assert(rows.exists(row ⇒ !row.contains("value") && row.getInt("key") == 3))
  }

  it should "return selected columns" in {
    val rdd = javaFunctions(sc).cassandraTable("java_api_test", "test_table")
      .select("key")
    assert(rdd.selectedColumnRefs().size === 1)
    assert(rdd.selectedColumnRefs().contains(new NamedColumnRef("key")))
  }

  it should "allow to use where clause to filter records" in {
    val rows = javaFunctions(sc).cassandraTable("java_api_test", "test_table")
      .where("value = ?", "two").collect()
    assert(rows.size === 1)
    assert(rows.exists(row => row.getString("value") == "two" && row.getInt("key") == 2))
  }

  it should "allow to read rows as an array of a single-column type supported by TypeConverter" in {
    val rows1 = javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table", mapColumnTo(classOf[java.lang.String]))
      .select("value")
      .collect()
    rows1 should have size 3
    rows1 should contain("one")
    rows1 should contain("two")
    rows1 should contain(null)

    val rows2 = javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table", mapColumnTo(classOf[java.lang.Integer]))
      .select("key")
      .collect()

    rows2 should have size 3
    rows2 should contain(1)
    rows2 should contain(2)
    rows2 should contain(3)

    val rows3 = javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table", mapColumnTo(classOf[java.lang.Double]))
      .select("key")
      .collect()

    rows3 should have size 3
    rows3 should contain(1d)
    rows3 should contain(2d)
    rows3 should contain(3d)
  }

  it should "allow to read rows as an array of a single-column list" in {
    val rows = javaFunctions(sc)
      .cassandraTable("java_api_test", "collections", mapColumnToListOf(classOf[String]))
      .select("l")
      .collect().map(_.toList)

    rows should have size 2
    rows should contain(List("item1", "item2"))
    rows should contain(List())
  }

  it should "allow to read rows as an array of a single-column set" in {
    val rows = javaFunctions(sc)
      .cassandraTable("java_api_test", "collections", mapColumnToSetOf(classOf[String]))
      .select("s")
      .collect().map(_.toSet)

    rows should have size 2
    rows should contain(Set("item1", "item2"))
    rows should contain(Set())
  }

  it should "allow to read rows as an array of a single-column map" in {
    val rows = javaFunctions(sc)
      .cassandraTable("java_api_test", "collections", mapColumnToMapOf(classOf[String], classOf[String]))
      .select("m")
      .collect().map(_.toMap)

    rows should have size 2
    rows should contain(Map("key1" → "value1", "key2" → "value2"))
    rows should contain(Map())
  }

  it should "allow to read rows as an array of multi-column type" in {
    val rows = javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table", mapRowTo(classOf[SampleJavaBean]))
      .collect().map(x => (x.getKey, x.getValue))

    rows should have size 3
    rows should contain((1, "one"))
    rows should contain((2, "two"))
    rows should contain((3, null))
  }

  it should "allow to read rows as an array of multi-column type with explicit column name mapping" in {
    val rows = javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table2", mapRowTo(classOf[SampleJavaBean],
        tuple.Pair.of("key", "some_key"), tuple.Pair.of("value", "some_value")))
      .collect().map(x => (x.getKey, x.getValue))

    rows should have size 3
    rows should contain((1, "one"))
    rows should contain((2, "two"))
    rows should contain((3, null))
  }

  it should "allow to read rows as an array of KV pairs of two single-column types" in {
    val rows = javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table",
        mapColumnTo(classOf[java.lang.Integer]), mapColumnTo(classOf[java.lang.String]))
      .collect()

    rows should have size 3
    rows should contain((1, "one"))
    rows should contain((2, "two"))
    rows should contain((3, null))
  }

  it should "allow to read rows as an array of KV pairs of a single-column type and a multi-column type" in {
    val rows = javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table",
        mapColumnTo(classOf[java.lang.Integer]), mapRowTo(classOf[SampleJavaBean]))
      .collect().map { case (i, x) ⇒ (i, (x.getKey, x.getValue))}

    rows should have size 3
    rows should contain((1, (1, "one")))
    rows should contain((2, (2, "two")))
    rows should contain((3, (3, null)))
  }

  it should "allow to read rows as an array of KV pairs of a multi-column type and a single-column type" in {
    val rows = javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table",
        mapRowTo(classOf[SampleJavaBean]), mapColumnTo(classOf[java.lang.Integer]))
      .collect().map { case (x, i) ⇒ ((x.getKey, x.getValue), i)}

    rows should have size 3
    rows should contain(((1, "one"), 1))
    rows should contain(((2, "two"), 2))
    rows should contain(((3, null), 3))
  }

  it should "allow to read rows as an array of KV pairs of multi-column types" in {
    val rows = javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table",
        mapRowTo(classOf[SampleJavaBean]), mapRowTo(classOf[SampleJavaBean]))
      .collect().map { case (x, y) ⇒ ((x.getKey, x.getValue), (y.getKey, y.getValue))}

    rows should have size 3
    rows should contain(((1, "one"), (1, "one")))
    rows should contain(((2, "two"), (2, "two")))
    rows should contain(((3, null), (3, null)))
  }

  it should "allow to read Cassandra data as array of Integer" in {
    val rows = javaFunctions(sc)
      .cassandraTable("java_api_test", "test_table", mapColumnTo(TypeConverter.JavaIntConverter))
      .select("key").collect()

    rows should have size 3
    rows should contain(1)
    rows should contain(2)
    rows should contain(3)
  }

  it should "allow to change the default Cassandra Connector to a custom one" in {
    // work with valid connector
    javaFunctions(sc).cassandraTable("java_api_test", "test_table").collect()

    // doesn't work with invalid connector
    val invalidConnector = CassandraConnector(Set(EmbeddedCassandra.cassandraHost), nativePort = 9999, rpcPort = 9998)
    intercept[IOException] {
      javaFunctions(sc).cassandraTable("java_api_test", "test_table").withConnector(invalidConnector).collect()
    }
  }

  it should "allow to read null columns" in {
    val row = javaFunctions(sc)
      .cassandraTable("java_api_test", "nulls")
      .select("i", "vi", "t", "d", "l")
      .first()

    row.getInt(0) should be (null)
    row.getVarInt(1) should be (null)
    row.getString(2) should be (null)
    row.getDate(3) should be (null)
    row.getList[Int](4) should be (new java.util.ArrayList[Int]())
  }

}
