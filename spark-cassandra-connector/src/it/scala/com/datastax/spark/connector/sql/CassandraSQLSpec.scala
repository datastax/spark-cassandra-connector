package com.datastax.spark.connector.sql

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.scalatest.{FlatSpec, Matchers}

class CassandraSQLSpec extends FlatSpec with Matchers with SharedEmbeddedCassandra with SparkTemplate {
  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(cassandraHost)

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

  }

  it should "allow to select all rows" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1").collect()
    result should have length 8
  }

  it should "allow to select rows with index columns" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE g = 2").collect()
    result should have length 4
  }

  it should "allow to select rows with >= clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE b >= 2").collect()
    result should have length 4
  }

  it should "allow to select rows with > clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE b > 2").collect()
    result should have length 0
  }

  it should "allow to select rows with < clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE b < 2").collect()
    result should have length 4
  }

  it should "allow to select rows with <= clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE b <= 2").collect()
    result should have length 8
  }

  it should "allow to select rows with in clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE b in (1,2)").collect()
    result should have length 8
  }

  it should "allow to select rows with or clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE b = 2 or b = 1").collect()
    result should have length 8
  }

  it should "allow to select rows with != clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE b != 2").collect()
    result should have length 4
  }

  it should "allow to select rows with <> clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE b <> 2").collect()
    result should have length 4
  }

  it should "allow to select rows with not in clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE b not in (1,2)").collect()
    result should have length 0
  }

  it should "allow to select rows with is not null clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE b is not null").collect()
    result should have length 8
  }

  it should "allow to select rows with like clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test2 WHERE name LIKE '%om' ").collect()
    result should have length 1
  }

   /* BETWEEN is not supported yet in Spark SQL
  it should "allow to select rows with between clause" in {
    val cc = new CassandraContext(sc)
    cc.setCqlKs("sql_test")
    val result = cc.sql("SELECT * FROM test2 WHERE a BETWEEN 1 AND 3 ").collect()
    result should have length 6
  }*/

  it should "allow to select rows with alias" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT a AS a_column, b AS b_column FROM test2").collect()
    result should have length 9
  }

  it should "allow to select rows with distinct column" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT DISTINCT a FROM test2").collect()
    result should have length 3
  }

  it should "allow to select rows with limit clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 limit 2").collect()
    result should have length 2
  }

  it should "allow to select rows with order by clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 order by d").collect()
    result should have length 8
  }

  it should "allow to select rows with group by clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT count(*) FROM test1 GROUP BY b").collect()
    result should have length 2
  }

  it should "allow to select rows with union clause" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test1.a FROM sql_test.test1 AS test1 UNION DISTINCT SELECT test2.a FROM sql_test.test2 AS test2").collect()
    result should have length 3
  }

  it should "allow to select rows with union distinct clause" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test1.a FROM sql_test.test1 AS test1 UNION DISTINCT SELECT test2.a FROM sql_test.test2 AS test2").collect()
    result should have length 3
  }

  it should "allow to select rows with union all clause" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test1.a FROM sql_test.test1 AS test1 UNION ALL SELECT test2.a FROM sql_test.test2 AS test2").collect()
    result should have length 17
  }

  it should "allow to select rows with having clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT count(*) FROM test1 GROUP BY b HAVING count(b) > 4").collect()
    result should have length 0
  }

  it should "allow to select rows with partition column clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE a = 1 and b = 1 and c = 1").collect()
    result should have length 4
  }

  it should "allow to select rows with partition column and cluster column clause" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test1 WHERE a = 1 and b = 1 and c = 1 and d = 1 and e = 1").collect()
    result should have length 1
  }

  it should "allow to insert into another table" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("INSERT INTO test3 SELECT a, b, c FROM test2").collect()
    val result2 = cc.sql("SELECT a, b, c FROM test3").collect()
    result2 should have length 9
  }

  it should "allow to insert into another table in different keyspace" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("INSERT INTO sql_test2.test3 as test3 SELECT test2.a, test2.b, test2.c FROM sql_test.test2 as test2").collect()
    val result2 = cc.sql("SELECT test3.a, test3.b, test3.c FROM sql_test2.test3 as test3").collect()
    result2 should have length 9
  }

  it should "allow to join two tables" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test1.a, test1.b, test1.c, test2.a FROM sql_test.test1 AS test1 " +
      "JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to join two tables from different keyspaces" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test1.a, test1.b, test1.c, test2.a FROM sql_test.test1 AS test1 " +
      "JOIN sql_test2.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to inner join two tables" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test1.a, test1.b, test1.c, test2.a FROM sql_test.test1 AS test1 " +
      "INNER JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 4
  }

  it should "allow to left join two tables" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test1.a, test1.b, test1.c, test1.d, test1.e, test1.f FROM sql_test.test1 AS test1 " +
      "LEFT JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 8
  }

  it should "allow to left outer join two tables" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test1.a, test1.b, test1.c, test1.d, test1.e, test1.f FROM sql_test.test1 AS test1 " +
      "LEFT OUTER JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 8
  }

  it should "allow to right join two tables" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test2.a, test2.b, test2.c FROM sql_test.test1 AS test1 " +
      "RIGHT JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 12
  }

  it should "allow to right outer join two tables" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test2.a, test2.b, test2.c FROM sql_test.test1 AS test1 " +
      "RIGHT OUTER JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 12
  }

  it should "allow to full join two tables" in {
    val cc = new CassandraSQLContext(sc)
    val result = cc.sql("SELECT test2.a, test2.b, test2.c FROM sql_test.test1 AS test1 " +
      "FULL JOIN sql_test.test2 AS test2 ON test1.a = test2.a AND test1.b = test2.b AND test1.c = test2.c").collect()
    result should have length 16
  }

  it should "allow to select rows for collection columns" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test_collection").collect()
    result should have length 1
  }

  it should "allow to select rows for data types of ASCII, INT, FLOAT, DOUBLE, BIGINT, BOOLEAN, DECIMAL, INET, TEXT, TIMESTAMP, UUID, VARINT" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("SELECT * FROM test_data_type").collect()
    result should have length 1
  }

  it should "allow to insert rows for data types of ASCII, INT, FLOAT, DOUBLE, BIGINT, BOOLEAN, DECIMAL, INET, TEXT, TIMESTAMP, UUID, VARINT" in {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
    val result = cc.sql("INSERT INTO test_data_type1 SELECT * FROM test_data_type").collect()
    val result1 = cc.sql("SELECT * FROM test_data_type1").collect()
    result1 should have length 1
  }
}
