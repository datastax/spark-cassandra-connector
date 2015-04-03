package com.datastax.spark.connector.sql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.scalatest.ConfigMap

class CassandraSQLPredicatePushdownSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
  var cc: CassandraSQLContext = null

  override def beforeAll() {
    cc = new CassandraSQLContext(sc)
    cc.setKeyspace("sql_test")
  }

  conn.withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS sql_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    session.execute("CREATE TABLE IF NOT EXISTS sql_test.test_pd (a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT, i INT, j INT, k INT, l INT, PRIMARY KEY ((a, b, c, d), e, f, g, h))")
    session.execute("USE sql_test")
    session.execute("CREATE INDEX test_pd_i ON test_pd(i)")
    session.execute("CREATE INDEX test_pd_j ON test_pd(j)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 1, 1, 1, 3, 4, 5, 6, 7, 8, 9, 10)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 1, 1, 1, 5, 6, 7, 8, 9, 10, 11, 12)")

    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 2, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 3, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 4, 1, 1, 3, 4, 5, 6, 7, 8, 9, 10)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 5, 1, 1, 5, 6, 7, 8, 9, 10, 11, 12)")

    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 2, 2, 1, 1, 2, 3, 4, 5, 6, 7, 8)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 3, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 4, 4, 1, 3, 4, 5, 6, 7, 8, 9, 10)")
    session.execute("INSERT INTO sql_test.test_pd (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (1, 5, 5, 1, 5, 6, 7, 8, 9, 10, 11, 12)")
  }

  it should "not allow predicate pushdown if some partition column doesn't have predicate" in {
    val result = cc.sql("select * from test_pd where a=1 and b=1 and c=1").collect()
  }

  it should "allow predicate pushdown if all partition columns have EQ predicate.  a=1 and b=1 and c=1 and d =1 are pushed down" in {
    val plan = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d=1").queryExecution.sparkPlan
    plan.nodeName shouldBe "CassandraTableScan"
    plan.children should have size 0
  }

  it should "allow predicate pushdown if all partition columns have EQ predicate except for last partition column has IN predicates." +
    " a=1 and b=1 and c=1 and d in (1, 2, 3, 4) are pushed down" in {
    val plan = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d in (1, 2, 3, 4)").queryExecution.sparkPlan
    plan.nodeName shouldBe "CassandraTableScan"
    plan.children should have size 0
  }

  it should "allow predicate pushdown if partition column predicates can be pushed down and last cluster column has EQ predicate." +
    " a=1 and b=1 and c=1 and d in (1, 2, 3, 4) and e=1 are pushed down" in {
    val plan = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d in (1, 2, 3, 4) and e=1").queryExecution.sparkPlan
    plan.nodeName shouldBe "CassandraTableScan"
    plan.children should have size 0
  }

  it should "allow predicate pushdown if partition column predicates can be pushed down and last cluster column has range predicate." +
    " a=1 and b=1 and c=1 and d in (1, 2, 3, 4) and e=1 and f>1 are pushed down" in {
    val plan = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d in (1, 2, 3, 4) and e=1 and f>1").queryExecution.sparkPlan
    plan.nodeName shouldBe "CassandraTableScan"
    plan.children should have size 0
  }

  it should "not allow predicate pushdown if partition column predicates can be pushed down and none-last cluster column has IN predicate." +
    " a=1 and b=1 and c=1 and d=3 and e=1 are pushed down" in {
    val result = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and e=1 and f in (1, 2, 3)").collect()
  }

  it should "not allow predicate pushdown if partition column predicates can be pushed down and none-last cluster column has IN predicate." +
    " a=1 and b=1 and c=1 and d=3 are pushed down" in {
    val result = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and h in (1, 2, 3)").collect()
  }

  it should "not allow predicate pushdown if partition column predicates can be pushed down and none-last cluster column has IN predicate." +
    " a=1 and b=1 and c=1 and d=3 and e in (1, 2, 3) are pushed down" in {
    val result = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and e in (1, 2, 3)").collect()
  }

  it should "allow predicate pushdown if there is only cluster column predicates." +
    " e=1 and f=3 are pushed down" in {
    val plan = cc.sql("select * from test_pd where e=1 and f=3").queryExecution.sparkPlan
    plan.nodeName shouldBe "CassandraTableScan"
    plan.children should have size 0
  }

  it should "not allow predicate pushdown if previous cluster column has None-EQ predicate." +
    " a=1 and b=1 and c=1 and d=3 and e=1 and f>2 are pushed down" in {
    val result = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d =3 and e=1 and f>2 and g=3").collect()
  }

  it should "not allow predicate pushdown if previous cluster column has None-EQ predicate." +
    " a=1 and b=1 and c=1 and d=3 and e>1 are pushed down" in {
    val result = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d =3 and e>1 and f>2 and g=3").collect()
  }

  it should "allow predicate pushdown if partition column predicates can be pushed down and only last cluster column has IN predicate." +
    " a=1 and b=1 and c=1 and d=3 and e=1 and f=1 and g=2 and h in (1, 2, 3) are pushed down" in {
    val plan = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and e=1 and f=1 and g=2 and h in (1, 2, 3)").queryExecution.sparkPlan
    plan.nodeName shouldBe "CassandraTableScan"
    plan.children should have size 0
  }

  it should "not allow predicate pushdown if previous cluster column has None-EQ predicate." +
    " a=1 and b=1 and c=1 and d=3 and e=1 and  f>1 are pushed down" in {
    val result = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and e=1 and f>1 and h in (1, 2, 3)").collect()
  }

  it should "allow predicate pushdown if there is EQ indexed column predicate. restriction on partition columns and cluster columns " +
    " can be loosed up. f=5 and i=4 are pushed down" in {
    val plan = cc.sql("select * from test_pd where f=5 and i=4").queryExecution.sparkPlan
    plan.nodeName shouldBe "CassandraTableScan"
    plan.children should have size 0
  }

  it should "not allow to push down cluster column IN predicate if there is EQ indexed column predicate. " +
    " i=4 are pushed down" in {
    val result = cc.sql("select * from test_pd where c=3 and g in (1,2,3) and i=4").collect()
  }

  it should "not allow to push down partition column IN predicate if there is EQ indexed column predicate. " +
    " i=4 are pushed down" in {
    val result = cc.sql("select * from test_pd where c=3 and d in (1,2,3) and i=4").collect()
  }

  it should "not allow to push down indexed column predicates if all indexed column predicates are none-EQ predicates. " in {
    val result = cc.sql("select * from test_pd where c=3 and i>4").collect()
  }

  it should "allow predicate pushdown if there is EQ indexed column predicates. " +
    " i=4 and j>5 are pushed down" in {
    val result = cc.sql("select * from test_pd where c=3 and i=4 and j>5").collect()
  }

  it should "not allow to push down none-primary column IN predicate. " +
    " e=3 is pushed down" in {
    val result = cc.sql("select * from test_pd where e=3 and j in (1,2,3)").collect()
  }

  it should "not allow to push down none-primary column predicates if there are no indexed column predicates. " +
    " a=1 and b=1 and c=1 and d=3 are pushed down" in {
    val result = cc.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and k=4").collect()
  }
}