package com.datastax.spark.connector.sql.source

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.cassandra.DefaultSource._

class CassandraDataSourceSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS sql_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test.test1 (a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT, PRIMARY KEY ((a, b, c), d , e, f))")
    session.execute("USE sql_test")
    session.execute("CREATE INDEX test1_g ON test1(g)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 1, 1, 1, 1)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 2, 1, 1, 2)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 1, 1, 2, 1)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 2, 1, 2, 2)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 1, 2, 1, 1)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 2, 2, 1, 2)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 1, 2, 2, 1)")
    session.execute("INSERT INTO sql_test.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 2, 2, 2, 2)")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test.test2 (a INT, b INT, c INT, name TEXT, PRIMARY KEY (a, b))")
    session.execute("INSERT INTO sql_test.test2 (a, b, c, name) VALUES (1, 1, 1, 'Tom')")
    session.execute("INSERT INTO sql_test.test2 (a, b, c, name) VALUES (1, 2, 3, 'Larry')")
    session.execute("INSERT INTO sql_test.test2 (a, b, c, name) VALUES (1, 3, 3, 'Henry')")
    session.execute("INSERT INTO sql_test.test2 (a, b, c, name) VALUES (2, 1, 3, 'Jerry')")
    session.execute("INSERT INTO sql_test.test2 (a, b, c, name) VALUES (2, 2, 3, 'Alex')")
    session.execute("INSERT INTO sql_test.test2 (a, b, c, name) VALUES (2, 3, 3, 'John')")
    session.execute("INSERT INTO sql_test.test2 (a, b, c, name) VALUES (3, 1, 3, 'Jack')")
    session.execute("INSERT INTO sql_test.test2 (a, b, c, name) VALUES (3, 2, 3, 'Hank')")
    session.execute("INSERT INTO sql_test.test2 (a, b, c, name) VALUES (3, 3, 3, 'Dug')")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test.test3 (a INT, b INT, c INT, PRIMARY KEY (a, b))")
    session.execute("CREATE KEYSPACE IF NOT EXISTS sql_test2 WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS sql_test2.test3 (a INT, b INT, c INT, PRIMARY KEY (a, b))")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test2.test2 (a INT, b INT, c INT, PRIMARY KEY (a, b))")
    session.execute("INSERT INTO sql_test2.test2 (a, b, c) VALUES (1, 1, 1)")
    session.execute("INSERT INTO sql_test2.test2 (a, b, c) VALUES (1, 2, 3)")
    session.execute("INSERT INTO sql_test2.test2 (a, b, c) VALUES (1, 3, 3)")
    session.execute("INSERT INTO sql_test2.test2 (a, b, c) VALUES (2, 1, 3)")
    session.execute("INSERT INTO sql_test2.test2 (a, b, c) VALUES (2, 2, 3)")
    session.execute("INSERT INTO sql_test2.test2 (a, b, c) VALUES (2, 3, 3)")
    session.execute("INSERT INTO sql_test2.test2 (a, b, c) VALUES (3, 1, 3)")
    session.execute("INSERT INTO sql_test2.test2 (a, b, c) VALUES (3, 2, 3)")
    session.execute("INSERT INTO sql_test2.test2 (a, b, c) VALUES (3, 3, 3)")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_data_type (a ASCII, b INT, c FLOAT, d DOUBLE, e BIGINT, f BOOLEAN, g DECIMAL, " +
      " h INET, i TEXT, j TIMESTAMP, k UUID, l VARINT, PRIMARY KEY ((a), b, c))")
    session.execute("INSERT INTO sql_test.test_data_type (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (" +
      "'ascii', 10, 12.34, 12.3456789, 123344556, true, 12.36, '74.125.239.135', 'text', '2011-02-03 04:05+0000', 123e4567-e89b-12d3-a456-426655440000 ,123456)")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_data_type1 (a ASCII, b INT, c FLOAT, d DOUBLE, e BIGINT, f BOOLEAN, g DECIMAL, " +
      " h INET, i TEXT, j TIMESTAMP, k UUID, l VARINT, PRIMARY KEY ((a), b, c))")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_collection (a INT, b SET<INT>, c MAP<INT, INT>, PRIMARY KEY (a))")
    session.execute("INSERT INTO sql_test.test_collection (a, b, c) VALUES (1, {1,2,3}, {1:2, 2:3})")

    session.execute("CREATE TYPE sql_test.address (street text, city text, zip int)")
    session.execute("CREATE TABLE IF NOT EXISTS sql_test.udts(key INT PRIMARY KEY, name text, addr frozen<address>)")
    session.execute("INSERT INTO sql_test.udts(key, name, addr) VALUES (1, 'name', {street: 'Some Street', city: 'Paris', zip: 11120})")
  }

  var sqlContext: SQLContext = null
  var scanType: String = null

  def setScanType() = {
    scanType = CassandraDataSourcePrunedFilteredScanTypeName
  }

  override def beforeAll() {
    sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.context.name", "testContext")
    setScanType()
    createTempTable("sql_test", "test1", "ddlTable")
    createTempTable("sql_test", "test2", "ddlTable2")
    createTempTable("sql_test2", "test3", "ddlTable3")
    createTempTable("sql_test", "test_collection", "test_collection")
    createTempTable("sql_test", "test_data_type", "test_data_type")
    createTempTable("sql_test", "test_data_type1", "test_data_type1")
  }

  override def afterAll() {
    super.afterAll()
    conn.withSessionDo { session =>
      session.execute("DROP KEYSPACE sql_test")
      session.execute("DROP KEYSPACE sql_test2")
    }
    sqlContext.dropTempTable("ddlTable")
    sqlContext.dropTempTable("ddlTable2")
    sqlContext.dropTempTable("ddlTable3")
    sqlContext.dropTempTable("test_collection")
    sqlContext.dropTempTable("test_data_type")
    sqlContext.dropTempTable("test_data_type1")
  }

  def createTempTable(keyspace: String, table: String, tmpTable: String) = {
    sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE $tmpTable
        |USING org.apache.spark.sql.cassandra
        |OPTIONS (
        | c_table "$table",
        | keyspace "$keyspace",
        | scan_type "$scanType"
        | )
      """.stripMargin.replaceAll("\n", " "))
  }

  it should "allow to select all rows" in {
    val result = sqlContext.cassandraTable("test1", "sql_test").select("a").collect()
    result should have length 8
    result.head should have length 1
  }

  it should "allow to register as a temp table" in {
    sqlContext.cassandraTable("test1", "sql_test").registerTempTable("test1")
    val temp = sqlContext.sql("SELECT * from test1").select("b").collect()
    temp should have length 8
    temp.head should have length 1
    sqlContext.dropTempTable("test1")
  }

  it should "allow to create a temp table" in {
    sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE tmpTable
        |USING org.apache.spark.sql.cassandra
        |OPTIONS (
        | c_table "test1",
        | keyspace "sql_test",
        | scan_type "$scanType",
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

  it should "allow to create a temp table with user defined schema" in {
    sqlContext.sql("SELECT * FROM ddlTable").collect() should have length 8
  }

  it should "allow to insert data into a cassandra table" in {
    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_insert (a INT PRIMARY KEY, b INT)")
    }
    createTempTable("sql_test", "test_insert", "insertTable")
    sqlContext.sql("SELECT * FROM insertTable").collect() should have length 0

    sqlContext.sql(
      s"""
        |INSERT OVERWRITE TABLE insertTable SELECT a, b FROM ddlTable
      """.stripMargin)
    sqlContext.sql("SELECT * FROM insertTable").collect() should have length 1
    sqlContext.dropTempTable("insertTable")
  }

  it should "allow to save data to a cassandra table" in {
    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_insert1 (a INT PRIMARY KEY, b INT)")
    }

    sqlContext.sql("SELECT a, b from ddlTable").save("org.apache.spark.sql.cassandra",
      ErrorIfExists, Map("c_table" -> "test_insert1", "keyspace" -> "sql_test"))

    sqlContext.cassandraTable("test_insert1", "sql_test").collect() should have length 1

    val message = intercept[UnsupportedOperationException] {
      sqlContext.sql("SELECT a, b from ddlTable").save("org.apache.spark.sql.cassandra",
        ErrorIfExists, Map("c_table" -> "test_insert1", "keyspace" -> "sql_test"))
    }.getMessage

    assert(
      message.contains("Writing to a none-empty Cassandra Table is not allowed."),
      "We should complain that 'Writing to a none-empty Cassandra Table is not allowed.'")
  }

  it should "not allow to overwrite a cassandra table" in {
    conn.withSessionDo { session =>
      session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_insert2 (a INT PRIMARY KEY, b INT)")
    }

    val message = intercept[UnsupportedOperationException] {
      sqlContext.sql("SELECT a, b from ddlTable").save("org.apache.spark.sql.cassandra",
        Overwrite, Map("c_table" -> "test_insert2", "keyspace" -> "sql_test"))
    }.getMessage

    assert(
      message.contains("Overwriting a Cassandra Table is not allowed."),
      "We should complain that 'Overwriting a Cassandra Table is not allowed.'")
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

  it should "allow to select rows with index columns" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE g = 2").collect()
    result should have length 4
  }

  it should "allow to select rows with >= clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE b >= 2").collect()
    result should have length 4
  }

  it should "allow to select rows with > clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE b > 2").collect()
    result should have length 0
  }

  it should "allow to select rows with < clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE b < 2").collect()
    result should have length 4
  }

  it should "allow to select rows with <= clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE b <= 2").collect()
    result should have length 8
  }

  it should "allow to select rows with in clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE b in (1,2)").collect()
    result should have length 8
  }

  it should "allow to select rows with in clause pushed down" in {
    val query = sqlContext.sql("SELECT * FROM ddlTable2 WHERE a in (1,2)")
    val result = query.collect()
    result should have length 6
  }

  it should "allow to select rows with or clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE b = 2 or b = 1").collect()
    result should have length 8
  }

  it should "allow to select rows with != clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE b != 2").collect()
    result should have length 4
  }

  it should "allow to select rows with <> clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE b <> 2").collect()
    result should have length 4
  }

  it should "allow to select rows with not in clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE b not in (1,2)").collect()
    result should have length 0
  }

  it should "allow to select rows with is not null clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable WHERE b is not null").collect()
    result should have length 8
  }

  it should "allow to select rows with like clause" in {
    val result = sqlContext.sql("SELECT * FROM ddlTable2 WHERE name LIKE '%om' ").collect()
    result should have length 1
  }

  it should "allow to insert into another table in different keyspace" in {
    val result = sqlContext.sql("INSERT INTO TABLE ddlTable3 SELECT test2.a, test2.b, test2.c FROM ddlTable2 as test2").collect()
    val result2 = sqlContext.sql("SELECT test3.a, test3.b, test3.c FROM ddlTable3 as test3").collect()
    result2 should have length 9
  }

  it should "allow to join two tables" in {
    val result = sqlContext.sql("SELECT test1.a, test1.b, test1.c, test2.a FROM ddlTable AS test1 " +
      "JOIN ddlTable2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to join two tables from different keyspaces" in {
    val result = sqlContext.sql("SELECT test1.a, test1.b, test1.c, test2.a FROM ddlTable AS test1 " +
      "JOIN ddlTable2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to inner join two tables" in {
    val result = sqlContext.sql("SELECT test1.a, test1.b, test1.c, test2.a FROM ddlTable AS test1 " +
      "INNER JOIN ddlTable2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to left join two tables" in {
    val result = sqlContext.sql("SELECT test1.a, test1.b, test1.c, test1.d, test1.e, test1.f FROM ddlTable AS test1 " +
      "LEFT JOIN ddlTable2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 8
  }

  it should "allow to left outer join two tables" in {
    val result = sqlContext.sql("SELECT test1.a, test1.b, test1.c, test1.d, test1.e, test1.f FROM ddlTable AS test1 " +
      "LEFT OUTER JOIN ddlTable2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 8
  }

  it should "allow to right join two tables" in {
    val result = sqlContext.sql("SELECT test2.a, test2.b, test2.c FROM ddlTable AS test1 " +
      "RIGHT JOIN ddlTable2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 12
  }

  it should "allow to right outer join two tables" in {
    val result = sqlContext.sql("SELECT test2.a, test2.b, test2.c FROM ddlTable AS test1 " +
      "RIGHT OUTER JOIN ddlTable2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 12
  }

  it should "allow to full join two tables" in {
    val result = sqlContext.sql("SELECT test2.a, test2.b, test2.c FROM ddlTable AS test1 " +
      "FULL JOIN ddlTable2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 16
  }

  it should "allow to select rows for collection columns" in {
    val result = sqlContext.sql("SELECT * FROM test_collection").collect()
    result should have length 1
  }

  it should "allow to select rows for data types of ASCII, INT, FLOAT, DOUBLE, BIGINT, BOOLEAN, DECIMAL, INET, TEXT, TIMESTAMP, UUID, VARINT" in {
    val result = sqlContext.sql("SELECT * FROM test_data_type").collect()
    result should have length 1
  }

  it should "allow to insert rows for data types of ASCII, INT, FLOAT, DOUBLE, BIGINT, BOOLEAN, DECIMAL, INET, TEXT, TIMESTAMP, UUID, VARINT" in {
    val result = sqlContext.sql("INSERT INTO TABLE test_data_type1 SELECT * FROM test_data_type").collect()
    val result1 = sqlContext.sql("SELECT * FROM test_data_type1").collect()
    result1 should have length 1
  }

  it should "allow to select specified non-UDT columns from a table containing some UDT columns" in {
    createTempTable("sql_test", "udts", "udts")
    val result = sqlContext.sql("SELECT key, name FROM udts").collect()
    result should have length 1
    val row = result.head
    row.getInt(0) should be(1)
    row.getString(1) should be ("name")
    sqlContext.dropTempTable("udts")
  }

  // Regression test for #454: java.util.NoSuchElementException thrown when accessing timestamp field using CassandraSQLContext
  it should "allow to restrict a clustering timestamp column value" in {
    conn.withSessionDo { session =>
      session.execute("create table sql_test.export_table(objectid int, utcstamp timestamp, service_location_id int, " +
        "service_location_name text, meterid int, primary key(meterid, utcstamp))")
    }
    createTempTable("sql_test", "export_table", "export_table")
    sqlContext.sql("select objectid, meterid, utcstamp  from export_table where meterid = 4317 and utcstamp > '2013-07-26 20:30:00-0700'").collect()
    sqlContext.dropTempTable("export_table")
  }

  it should "allow to min/max timestamp column" in {
    conn.withSessionDo { session =>
      session.execute("create table sql_test.timestamp_conversion_bug (k int, v int, d timestamp, primary key(k,v))")
      session.execute("insert into sql_test.timestamp_conversion_bug (k, v, d) values (1, 1, '2015-01-03 15:13')")
      session.execute("insert into sql_test.timestamp_conversion_bug (k, v, d) values (1, 2, '2015-01-03 16:13')")
      session.execute("insert into sql_test.timestamp_conversion_bug (k, v, d) values (1, 3, '2015-01-03 17:13')")
      session.execute("insert into sql_test.timestamp_conversion_bug (k, v, d) values (1, 4, '2015-01-03 18:13')")
    }

    createTempTable("sql_test", "timestamp_conversion_bug", "timestamp_conversion_bug")
    sqlContext.sql("select k, min(d), max(d) from timestamp_conversion_bug group by k").collect()
    sqlContext.dropTempTable("timestamp_conversion_bug")
  }

}
