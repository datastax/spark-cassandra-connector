package com.datastax.spark.connector.sql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate._

import org.apache.spark.sql.cassandra.CassandraSQLContext

class CassandraSQLSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(defaultConf)
  var cc: CassandraSQLContext = null

  conn.withSessionDo { session =>
    session.execute("DROP KEYSPACE IF EXISTS sql_test")
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
      " h INET, i TEXT, j TIMESTAMP, k UUID, l VARINT, m SMALLINT, n TINYINT,  PRIMARY KEY ((a), b, c))")
    session.execute("INSERT INTO sql_test.test_data_type (a, b, c, d, e, f, g, h, i, j, k, l, m, n) VALUES (" +
      "'ascii', 10, 12.34, 12.3456789, 123344556, true, 12.36, '74.125.239.135', 'text', '2011-02-03 04:05+0000', 123e4567-e89b-12d3-a456-426655440000 ,123456, 22, 4)")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_data_type1 (a ASCII, b INT, c FLOAT, d DOUBLE, e BIGINT, f BOOLEAN, g DECIMAL, " +
      " h INET, i TEXT, j TIMESTAMP, k UUID, l VARINT, m SMALLINT, n TINYINT, PRIMARY KEY ((a), b, c))")

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS sql_test.test_collection
         | (a INT, b SET<TIMESTAMP>, c MAP<TIMESTAMP, TIMESTAMP>, d List<TIMESTAMP>, PRIMARY KEY (a))
      """.stripMargin.replaceAll("\n", " "))
    session.execute(
      s"""
         |INSERT INTO sql_test.test_collection (a, b, c, d)
         |VALUES (1,
         |        {'2011-02-03','2011-02-04'},
         |        {'2011-02-03':'2011-02-04', '2011-02-06':'2011-02-07'},
         |        ['2011-02-03','2011-02-04'])
      """.stripMargin.replaceAll("\n", " "))

    session.execute("CREATE TYPE sql_test.address (street text, city text, zip int, date TIMESTAMP)")
    session.execute("CREATE TABLE IF NOT EXISTS sql_test.udts(key INT PRIMARY KEY, name text, addr frozen<address>)")
    session.execute("INSERT INTO sql_test.udts(key, name, addr) VALUES (1, 'name', {street: 'Some Street', city: 'Paris', zip: 11120})")

    session.execute(
      s"""
         |CREATE TYPE IF NOT EXISTS sql_test.category_metadata (
         |  category_id text,
         |  metric_descriptors list <text>
         |)
      """.stripMargin.replaceAll("\n", " "))
    session.execute(
      s"""
         |CREATE TYPE IF NOT EXISTS sql_test.object_metadata (
         |  name text,
         |  category_metadata frozen<category_metadata>,
         |  bucket_size int
         |)
      """.stripMargin.replaceAll("\n", " "))
    session.execute(
      s"""
         |CREATE TYPE IF NOT EXISTS sql_test.relation (
         |  type text,
         |  object_type text,
         |  related_to text,
         |  obj_id text
         |)
      """.stripMargin.replaceAll("\n", " "))
    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS sql_test.objects (
         |  obj_id text,
         |  metadata  frozen<object_metadata>,
         |  relations list<frozen<relation>>,
         |  ts timestamp, PRIMARY KEY(obj_id)
         |)
      """.stripMargin.replaceAll("\n", " "))
    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS sql_test.objects_copy (
         |  obj_id text,
         |  metadata  frozen<object_metadata>,
         |  relations list<frozen<relation>>,
         |  ts timestamp, PRIMARY KEY(obj_id)
         |)
      """.stripMargin.replaceAll("\n", " "))
    session.execute(
      s"""
         |INSERT INTO sql_test.objects (obj_id, ts, metadata, relations)
         |values (
         |  '123', '2015-06-16 15:53:23-0400',
         |  {
         |    name: 'foo',
         |    category_metadata: {
         |      category_id: 'thermostat',
         |      metric_descriptors: []
         |    },
         |    bucket_size: 0
         |  },
         |  [
         |    {
         |      type: 'a',
         |      object_type: 'b',
         |      related_to: 'c',
         |      obj_id: 'd'
         |    },
         |    {
         |      type: 'a1',
         |      object_type: 'b1',
         |      related_to: 'c1',
         |      obj_id: 'd1'
         |    }
         |  ]
         |)
      """.stripMargin.replaceAll("\n", " "))
  }

  override def beforeAll() {
    super.beforeAll()
    cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
  }

  it should "allow to select all rows" in {
    val result = cc.sql("SELECT * FROM test1").collect()
    result should have length 8
  }

  it should "allow to select rows with index columns" in {
    val result = cc.sql("SELECT * FROM test1 WHERE g = 2").collect()
    result should have length 4
  }

  it should "allow to select rows with >= clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE b >= 2").collect()
    result should have length 4
  }

  it should "allow to select rows with > clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE b > 2").collect()
    result should have length 0
  }

  it should "allow to select rows with < clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE b < 2").collect()
    result should have length 4
  }

  it should "allow to select rows with <= clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE b <= 2").collect()
    result should have length 8
  }

  it should "allow to select rows with in clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE b in (1,2)").collect()
    result should have length 8
  }

  it should "allow to select rows with in clause pushed down" in {
    val query = cc.sql("SELECT * FROM test2 WHERE a in (1,2)")
    query.queryExecution.sparkPlan.toString should include regex """PushedFilter: \[In\(a.*\)\]""".r
    val result = query.collect()
    result should have length 6
  }

  it should "allow to select rows with or clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE b = 2 or b = 1").collect()
    result should have length 8
  }

  it should "allow to select rows with != clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE b != 2").collect()
    result should have length 4
  }

  it should "allow to select rows with <> clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE b <> 2").collect()
    result should have length 4
  }

  it should "allow to select rows with not in clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE b not in (1,2)").collect()
    result should have length 0
  }

  it should "allow to select rows with is not null clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE b is not null").collect()
    result should have length 8
  }

  it should "allow to select rows with like clause" in {
    val result = cc.sql("SELECT * FROM test2 WHERE name LIKE '%om' ").collect()
    result should have length 1
  }

  it should "allow to select rows with between clause" in {
    val result = cc.sql("SELECT * FROM test2 WHERE a BETWEEN 1 AND 2 ").collect()
    result should have length 6
  }

  it should "allow to select rows with alias" in {
    val result = cc.sql("SELECT a AS a_column, b AS b_column FROM test2").collect()
    result should have length 9
  }

  it should "allow to select rows with distinct column" in {
    val result = cc.sql("SELECT DISTINCT a FROM test2").collect()
    result should have length 3
  }

  it should "allow to select rows with limit clause" in {
    val result = cc.sql("SELECT * FROM test1 limit 2").collect()
    result should have length 2
  }

  it should "allow to select rows with order by clause" in {
    val result = cc.sql("SELECT * FROM test1 order by d").collect()
    result should have length 8
  }

  it should "allow to select rows with group by clause" in {
    val result = cc.sql("SELECT count(*) FROM test1 GROUP BY b").collect()
    result should have length 2
  }

  it should "allow to select rows with union clause" in {
    val result = cc.sql("SELECT test1.a FROM sql_test.test1 AS test1 UNION DISTINCT SELECT test2.a FROM sql_test.test2 AS test2").collect()
    result should have length 3
  }

  it should "allow to select rows with union distinct clause" in {
    val result = cc.sql("SELECT test1.a FROM sql_test.test1 AS test1 UNION DISTINCT SELECT test2.a FROM sql_test.test2 AS test2").collect()
    result should have length 3
  }

  it should "allow to select rows with union all clause" in {
    val result = cc.sql("SELECT test1.a FROM sql_test.test1 AS test1 UNION ALL SELECT test2.a FROM sql_test.test2 AS test2").collect()
    result should have length 17
  }

  it should "allow to select rows with having clause" in {
    val result = cc.sql("SELECT count(*) FROM test1 GROUP BY b HAVING count(b) > 4").collect()
    result should have length 0
  }

  it should "allow to select rows with partition column clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE a = 1 and b = 1 and c = 1").collect()
    result should have length 4
  }

  it should "allow to select rows with partition column and cluster column clause" in {
    val result = cc.sql("SELECT * FROM test1 WHERE a = 1 and b = 1 and c = 1 and d = 1 and e = 1").collect()
    result should have length 1
  }

  it should "allow to insert into another table" in {
    val result = cc.sql("INSERT INTO TABLE test3 SELECT a, b, c FROM test2").collect()
    val result2 = cc.sql("SELECT a, b, c FROM test3").collect()
    result2 should have length 9
  }

  it should "allow to insert into another table in different keyspace" in {
    val result = cc.sql("INSERT INTO TABLE sql_test2.test3 SELECT test2.a, test2.b, test2.c FROM sql_test.test2 as test2").collect()
    val result2 = cc.sql("SELECT test3.a, test3.b, test3.c FROM sql_test2.test3 as test3").collect()
    result2 should have length 9
  }

  it should "allow to join two tables" in {
    val result = cc.sql("SELECT test1.a, test1.b, test1.c, test2.a FROM sql_test.test1 AS test1 " +
      "JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to join two tables from different keyspaces" in {
    val result = cc.sql("SELECT test1.a, test1.b, test1.c, test2.a FROM sql_test.test1 AS test1 " +
      "JOIN sql_test2.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to inner join two tables" in {
    val result = cc.sql("SELECT test1.a, test1.b, test1.c, test2.a FROM sql_test.test1 AS test1 " +
      "INNER JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to left join two tables" in {
    val result = cc.sql("SELECT test1.a, test1.b, test1.c, test1.d, test1.e, test1.f FROM sql_test.test1 AS test1 " +
      "LEFT JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 8
  }

  it should "allow to left outer join two tables" in {
    val result = cc.sql("SELECT test1.a, test1.b, test1.c, test1.d, test1.e, test1.f FROM sql_test.test1 AS test1 " +
      "LEFT OUTER JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 8
  }

  it should "allow to right join two tables" in {
    val result = cc.sql("SELECT test2.a, test2.b, test2.c FROM sql_test.test1 AS test1 " +
      "RIGHT JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 12
  }

  it should "allow to right outer join two tables" in {
    val result = cc.sql("SELECT test2.a, test2.b, test2.c FROM sql_test.test1 AS test1 " +
      "RIGHT OUTER JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 12
  }

  it should "allow to full join two tables" in {
    val result = cc.sql("SELECT test2.a, test2.b, test2.c FROM sql_test.test1 AS test1 " +
      "FULL JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 16
  }

  it should "allow to select rows for collection columns" in {
    val result = cc.sql("SELECT * FROM test_collection").collect()
    result should have length 1
  }

  it should "allow to select rows for data types of ASCII, INT, FLOAT, DOUBLE, BIGINT, BOOLEAN, DECIMAL, INET, TEXT, TIMESTAMP, UUID, VARINT, SMALLINT" in {
    val result = cc.sql("SELECT * FROM test_data_type").collect()
    result should have length 1
  }

  it should "allow to insert rows for data types of ASCII, INT, FLOAT, DOUBLE, BIGINT, BOOLEAN, DECIMAL, INET, TEXT, TIMESTAMP, UUID, VARINT, SMALLINT" in {
    val result = cc.sql("INSERT INTO TABLE test_data_type1 SELECT * FROM test_data_type").collect()
    val result1 = cc.sql("SELECT * FROM test_data_type1").collect()
    result1 should have length 1
  }

  it should "allow to select specified non-UDT columns from a table containing some UDT columns" in {
    val result = cc.sql("SELECT key, name FROM udts").collect()
    result should have length 1
    val row = result.head
    row.getInt(0) should be(1)
    row.getString(1) should be ("name")
  }

  //TODO: SPARK-9269 is opened to address Set matching issue. I change the Set data type to List for now
  it should "allow to select UDT collection column and nested UDT column" in {

    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "objects",
          "keyspace" -> "sql_test"
        )
      )
      .load()
      .collect()
    result should have length 1
  }

  it should "allow writing UDTs to C* tables" in {

    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "objects",
          "keyspace" -> "sql_test"
        )
      )
      .load()
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "objects_copy",
          "keyspace" -> "sql_test"
        )
      ).save()
  }

  // Regression test for #454: java.util.NoSuchElementException thrown when accessing timestamp field using CassandraSQLContext
  it should "allow to restrict a clustering timestamp column value" in {
    conn.withSessionDo { session =>
      session.execute("create table sql_test.export_table(objectid int, utcstamp timestamp, service_location_id int, " +
        "service_location_name text, meterid int, primary key(meterid, utcstamp))")
    }
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    cc.cassandraSql("select objectid, meterid, utcstamp  from export_table where meterid = 4317 and utcstamp > '2013-07-26 20:30:00-0700'").collect()
  }

  it should "allow to min/max timestamp column" in {
    conn.withSessionDo { session =>
      session.execute("create table sql_test.timestamp_conversion_bug (k int, v int, d timestamp, primary key(k,v))")
      session.execute("insert into sql_test.timestamp_conversion_bug (k, v, d) values (1, 1, '2015-01-03 15:13')")
      session.execute("insert into sql_test.timestamp_conversion_bug (k, v, d) values (1, 2, '2015-01-03 16:13')")
      session.execute("insert into sql_test.timestamp_conversion_bug (k, v, d) values (1, 3, '2015-01-03 17:13')")
      session.execute("insert into sql_test.timestamp_conversion_bug (k, v, d) values (1, 4, '2015-01-03 18:13')")
    }
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    cc.cassandraSql("select k, min(d), max(d) from timestamp_conversion_bug group by k").collect()
  }

  it should "be able to push down filter on UUID and Inet columns" in {
    conn.withSessionDo { session =>
      session.execute("create table sql_test.uuid_inet_type (a UUID, b INET, c INT, primary key(a,b))")
      session.execute("insert into sql_test.uuid_inet_type (a, b, c) " +
        "values (123e4567-e89b-12d3-a456-426655440000,'74.125.239.135', 1)")
      session.execute("insert into sql_test.uuid_inet_type (a, b, c) " +
        "values (067e6162-3b6f-4ae2-a171-2470b63dff00, '74.125.239.136', 2)")
    }
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.cassandraSql(
      "select * " +
        "from uuid_inet_type " +
        "where b > '74.125.239.135'").collect()
    result should have length 1
    val result1 = cc.cassandraSql(
      "select * " +
        "from uuid_inet_type " +
        "where a < '123e4567-e89b-12d3-a456-426655440000'").collect()
    result1 should have length 1
    val result2 = cc.cassandraSql(
      "select * " +
        "from uuid_inet_type " +
        "where a = '123e4567-e89b-12d3-a456-426655440000' and b = '74.125.239.135'").collect()
    result2 should have length 1
  }

  it should "be able to push down filter on varint columns" in {
    conn.withSessionDo { session =>
      session.execute(
        s"""
        |CREATE TABLE sql_test.varint_test(
        |  id varint,
        |  series varint,
        |  rollup_minutes varint,
        |  event text,
        |  PRIMARY KEY ((id, series, rollup_minutes), event)
        |)
      """.stripMargin.replaceAll("\n", " "))
      session.execute(
        s"""
        |INSERT INTO sql_test.varint_test(id, series, rollup_minutes, event)
        |VALUES(1234567891234, 1234567891235, 1234567891236, 'event')
      """.stripMargin.replaceAll("\n", " "))
    }
    val cc = new CassandraSQLContext(sc)
    cc.sql(
      s"""
        |SELECT * FROM sql_test.varint_test
        |WHERE id = 1234567891234
        | AND series = 1234567891235
        | AND rollup_minutes = 1234567891236
      """.stripMargin.replaceAll("\n", " ")
    ).collect()  should have length 1
  }
}
