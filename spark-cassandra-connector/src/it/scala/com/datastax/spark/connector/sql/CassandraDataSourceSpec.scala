package com.datastax.spark.connector.sql

import org.apache.spark.Logging
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.SaveMode._

import org.apache.spark.sql.cassandra.{TableRef, CassandraSourceRelation}

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate._
import com.datastax.spark.connector.embedded.EmbeddedCassandra._

class CassandraDataSourceSpec extends SparkCassandraITFlatSpecBase with Logging {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(defaultConf)
  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS sql_ds_test WITH REPLICATION = " +
      "{ 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS sql_ds_test.test1 (a INT, b INT, c INT, d INT, e INT, f INT, g INT, " +
      "h INT, PRIMARY KEY ((a, b, c), d , e, f))")
    session.execute("USE sql_ds_test")
    session.execute("INSERT INTO sql_ds_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 1, 1, 1, 1)")
    session.execute("INSERT INTO sql_ds_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 2, 1, 1, 2)")
    session.execute("INSERT INTO sql_ds_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 1, 1, 2, 1)")
    session.execute("INSERT INTO sql_ds_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 2, 1, 2, 2)")
    session.execute("INSERT INTO sql_ds_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 1, 2, 1, 1)")
    session.execute("INSERT INTO sql_ds_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 2, 2, 1, 2)")
    session.execute("INSERT INTO sql_ds_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 1, 2, 2, 1)")
    session.execute("INSERT INTO sql_ds_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 2, 2, 2, 2)")
  }

  val sqlContext: SQLContext = new SQLContext(sc)
  def pushDown: Boolean = true

  override def beforeAll() {
    createTempTable("sql_ds_test", "test1", "tmpTable")
  }

  override def afterAll() {
    super.afterAll()
    conn.withSessionDo { session =>
      session.execute("DROP KEYSPACE sql_ds_test")
    }
    sqlContext.dropTempTable("tmpTable")
  }

  def createTempTable(keyspace: String, table: String, tmpTable: String) = {
    sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE $tmpTable
        |USING org.apache.spark.sql.cassandra
        |OPTIONS (
        | c_table "$table",
        | keyspace "$keyspace",
        | push_down "$pushDown",
        | spark_cassandra_input_page_row_size "10",
        | spark_cassandra_output_consistency_level "ONE",
        | spark_cassandra_connection_timeout_ms "1000",
        | spark_cassandra_connection_host "${getHost(0).getHostAddress}"
        | )
      """.stripMargin.replaceAll("\n", " "))
  }

  def cassandraTable(tableRef: TableRef) : DataFrame = {
    sqlContext.baseRelationToDataFrame(CassandraSourceRelation(tableRef, sqlContext))
  }

  it should "allow to select all rows" in {
    val result = cassandraTable(TableRef("test1", "sql_ds_test")).select("a").collect()
    result should have length 8
    result.head should have length 1
  }

  it should "allow to register as a temp table" in {
    cassandraTable(TableRef("test1", "sql_ds_test")).registerTempTable("test1")
    val temp = sqlContext.sql("SELECT * from test1").select("b").collect()
    temp should have length 8
    temp.head should have length 1
    sqlContext.dropTempTable("test1")
  }

  it should "allow to insert data into a cassandra table" in {
    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS sql_ds_test.test_insert (a INT PRIMARY KEY, b INT)")
    }
    createTempTable("sql_ds_test", "test_insert", "insertTable")
    sqlContext.sql("SELECT * FROM insertTable").collect() should have length 0

    sqlContext.sql("INSERT OVERWRITE TABLE insertTable SELECT a, b FROM tmpTable")
    sqlContext.sql("SELECT * FROM insertTable").collect() should have length 1
    sqlContext.dropTempTable("insertTable")
  }

  it should "allow to save data to a cassandra table" in {
    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS sql_ds_test.test_insert1 (a INT PRIMARY KEY, b INT)")
    }

    sqlContext.sql("SELECT a, b from tmpTable").save(
      "org.apache.spark.sql.cassandra",
      ErrorIfExists,
      Map("c_table" -> "test_insert1", "keyspace" -> "sql_ds_test"))

    cassandraTable(TableRef("test_insert1", "sql_ds_test")).collect() should have length 1

    val message = intercept[UnsupportedOperationException] {
      sqlContext.sql("SELECT a, b from tmpTable").save(
        "org.apache.spark.sql.cassandra",
        ErrorIfExists,
        Map("c_table" -> "test_insert1", "keyspace" -> "sql_ds_test"))
    }.getMessage

    assert(
      message.contains("Writing to a non-empty Cassandra Table is not allowed."),
      "We should complain that 'Writing to a non-empty Cassandra table is not allowed.'")
  }

  it should "allow to overwrite a cassandra table" in {
    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS sql_ds_test.test_insert2 (a INT PRIMARY KEY, b INT)")
      session.execute("INSERT INTO sql_ds_test.test_insert2 (a, b) VALUES (3,4)")
      session.execute("INSERT INTO sql_ds_test.test_insert2 (a, b) VALUES (5,6)")
    }

    sqlContext.sql("SELECT a, b from tmpTable").save(
      "org.apache.spark.sql.cassandra",
      Overwrite,
      Map("c_table" -> "test_insert2", "keyspace" -> "sql_ds_test"))
    createTempTable("sql_ds_test", "test_insert2", "insertTable2")
    sqlContext.sql("SELECT * FROM insertTable2").collect() should have length 1
    sqlContext.dropTempTable("insertTable2")
  }

  it should "allow to filter a table" in {
    sqlContext.sql("SELECT a, b FROM tmpTable WHERE a=1 and b=2 and c=1 and e=1").collect() should have length 2
  }

  it should "allow to filter a table with a function for a column alias" in {
    sqlContext.sql("SELECT * FROM (SELECT (a + b + c) AS x, d FROM tmpTable) " +
      "AS tmpTable1 WHERE x= 3").collect() should have length 4
  }

  it should "allow to filter a table with alias" in {
    sqlContext.sql("SELECT * FROM (SELECT a AS a1, b AS b1, c AS c1, d AS d1, e AS e1" +
      " FROM tmpTable) AS tmpTable1 WHERE  a1=1 and b1=2 and c1=1 and e1=1 ").collect() should have length 2
  }

  it should "be able to save DF with reversed order columns to a Cassandra table" in {
    conn.withSessionDo { session =>
      session.execute(
        s"""
        |CREATE TABLE sql_ds_test.df_test(
        |  customer_id int,
        |  uri text,
        |  browser text,
        |  epoch bigint,
        |  PRIMARY KEY (customer_id, epoch, uri)
        |)
      """.stripMargin.replaceAll("\n", " "))
    }
    val test_df = Test(1400820884, "http://foobar", "Firefox", 123242)

    import sqlContext.implicits._
    val df = sc.parallelize(Seq(test_df)).toDF
    df.save(
      "org.apache.spark.sql.cassandra",
      SaveMode.Overwrite,
      options = Map( "c_table" -> "df_test", "keyspace" -> "sql_ds_test"))
    cassandraTable(TableRef("df_test", "sql_ds_test")).collect() should have length 1
  }

  it should "be able to save DF with partial columns to a Cassandra table" in {
    conn.withSessionDo { session =>
      session.execute(
        s"""
        |CREATE TABLE sql_ds_test.df_test2(
        |  customer_id int,
        |  uri text,
        |  browser text,
        |  epoch bigint,
        |  PRIMARY KEY (customer_id, epoch)
        |)
      """.stripMargin.replaceAll("\n", " "))
    }
    val test_df = TestPartialColumns(1400820884, "Firefox", 123242)

    import sqlContext.implicits._
    val df = sc.parallelize(Seq(test_df)).toDF
    df.save(
      "org.apache.spark.sql.cassandra",
      SaveMode.Overwrite,
      options = Map( "c_table" -> "df_test2", "keyspace" -> "sql_ds_test"))
    cassandraTable(TableRef("df_test2", "sql_ds_test")).collect() should have length 1
  }
}

case class Test(val epoch:Long, val uri:String, val browser:String, val customer_id:Int)
case class TestPartialColumns(val epoch:Long, val browser:String, val customer_id:Int)