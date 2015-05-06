package com.datastax.spark.connector.sql.source

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.cassandra.CassandraDefaultSource._

class CassandraDataSourceSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS sql_test WITH REPLICATION = " +
      "{ 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test.test1 (a INT, b INT, c INT, d INT, e INT, f INT, g INT, " +
      "h INT, PRIMARY KEY ((a, b, c), d , e, f))")
    session.execute("USE sql_test")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 1, 1, 1, 1)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 2, 1, 1, 2)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 1, 1, 2, 1)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 2, 1, 2, 2)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 1, 2, 1, 1)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 2, 2, 1, 2)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 1, 2, 2, 1)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 2, 2, 2, 2)")
  }

  val sqlContext: SQLContext = new SQLContext(sc)
  def pushDown: Boolean = true

  override def beforeAll() {
    createTempTable("sql_test", "test1", "ddlTable")
  }

  override def afterAll() {
    super.afterAll()
    conn.withSessionDo { session =>
      session.execute("DROP KEYSPACE sql_test")
    }
    sqlContext.dropTempTable("ddlTable")
  }

  def createTempTable(keyspace: String, table: String, tmpTable: String) = {
    sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE $tmpTable
        |USING org.apache.spark.sql.cassandra
        |OPTIONS (
        | c_table "$table",
        | keyspace "$keyspace",
        | push_down "$pushDown"
        | )
      """.stripMargin.replaceAll("\n", " "))
  }

  it should "allow to select all rows" in {
    val result = sqlContext.cassandraTable(TableIdent("test1", "sql_test")).select("a").collect()
    result should have length 8
    result.head should have length 1
  }

  it should "allow to register as a temp table" in {
    sqlContext.cassandraTable(TableIdent("test1", "sql_test")).registerTempTable("test1")
    val temp = sqlContext.sql("SELECT * from test1").select("b").collect()
    temp should have length 8
    temp.head should have length 1
    sqlContext.dropTempTable("test1")
  }

  it should "allow to create a temp table with user defined schema" in {
    sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE tmpTable
        |USING org.apache.spark.sql.cassandra
        |OPTIONS (
        | c_table "test1",
        | keyspace "sql_test",
        | push_down "$pushDown",
        | schema '{"type":"struct","fields":
        | [{"name":"a","type":"integer","nullable":true,"metadata":{}},
        | {"name":"b","type":"integer","nullable":true,"metadata":{}},
        | {"name":"c","type":"integer","nullable":true,"metadata":{}},
        | {"name":"d","type":"integer","nullable":true,"metadata":{}},
        | {"name":"e","type":"integer","nullable":true,"metadata":{}},
        | {"name":"f","type":"integer","nullable":true,"metadata":{}},
        | {"name":"g","type":"integer","nullable":true,"metadata":{}},
        | {"name":"h","type":"integer","nullable":true,"metadata":{}}]}'
        | )
      """.stripMargin.replaceAll("\n", " "))
    sqlContext.sql("SELECT * FROM tmpTable").collect() should have length 8
    sqlContext.dropTempTable("tmpTable")
  }

  it should "allow to create a temp table" in {
    sqlContext.sql("SELECT * FROM ddlTable").collect() should have length 8
  }

  it should "allow to insert data into a cassandra table" in {
    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_insert (a INT PRIMARY KEY, b INT)")
    }
    createTempTable("sql_test", "test_insert", "insertTable")
    sqlContext.sql("SELECT * FROM insertTable").collect() should have length 0

    sqlContext.sql("INSERT OVERWRITE TABLE insertTable SELECT a, b FROM ddlTable")
    sqlContext.sql("SELECT * FROM insertTable").collect() should have length 1
    sqlContext.dropTempTable("insertTable")
  }

  it should "allow to save data to a cassandra table" in {
    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_insert1 (a INT PRIMARY KEY, b INT)")
    }

    sqlContext.sql("SELECT a, b from ddlTable").save("org.apache.spark.sql.cassandra",
      ErrorIfExists, Map("c_table" -> "test_insert1", "keyspace" -> "sql_test"))

    sqlContext.cassandraTable(TableIdent("test_insert1", "sql_test")).collect() should have length 1

    val message = intercept[UnsupportedOperationException] {
      sqlContext.sql("SELECT a, b from ddlTable").save("org.apache.spark.sql.cassandra",
        ErrorIfExists, Map("c_table" -> "test_insert1", "keyspace" -> "sql_test"))
    }.getMessage

    assert(
      message.contains("Writing to a none-empty Cassandra Table is not allowed."),
      "We should complain that 'Writing to a none-empty Cassandra Table is not allowed.'")
  }

  it should "allow to overwrite a cassandra table" in {
    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_insert2 (a INT PRIMARY KEY, b INT)")
    }

    sqlContext.sql("SELECT a, b from ddlTable").save("org.apache.spark.sql.cassandra",
      Overwrite, Map("c_table" -> "test_insert2", "keyspace" -> "sql_test"))
  }

  it should "allow to filter a table" in {
    sqlContext.sql("SELECT a, b FROM ddlTable WHERE a=1 and b=2 and c=1 and e=1").collect() should have length 2
  }

  it should "allow to filter a table with a function for a column alias" in {
    sqlContext.sql("SELECT * FROM (SELECT (a + b + c) AS x, d FROM ddlTable) " +
      "AS ddlTable41 WHERE x= 3").collect() should have length 4
  }

  it should "allow to filter a table with alias" in {
    sqlContext.sql("SELECT * FROM (SELECT a AS a1, b AS b1, c AS c1, d AS d1, e AS e1" +
      " FROM ddlTable) AS ddlTable51 WHERE  a1=1 and b1=2 and c1=1 and e1=1 ").collect() should have length 2
  }

  it should "allow to filter a table with alias2" in {
    sqlContext.sql("SELECT * FROM (SELECT a AS a1, b AS b1, c AS c1, d AS d1, e AS e1" +
      " FROM ddlTable) AS ddlTable51 WHERE  a1=1 and b1=2 and c1=1 and e1 in (1,2) ").collect() should have length 4
  }
}
