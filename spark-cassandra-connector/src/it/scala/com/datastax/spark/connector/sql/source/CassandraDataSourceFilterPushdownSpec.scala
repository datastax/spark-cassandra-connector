package com.datastax.spark.connector.sql.source

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra.CassandraDefaultSource._

class CassandraDataSourceFilterPushdownSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultSparkConf)

  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

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

  val sqlContext: SQLContext = new SQLContext(sc)
  def pushDown: Boolean = true

  override def beforeAll() {
    createTempTable("sql_test", "test_pd", "test_pd")
  }

  override def afterAll() {
    super.afterAll()
    conn.withSessionDo { session =>
      session.execute("DROP KEYSPACE sql_test")
    }
    sqlContext.dropTempTable("test_pd")
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

  it should "not allow filter pushdown if some partition column doesn't have filter" in {
    val result = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1").collect()
  }

  it should "allow filter pushdown if all partition columns have EQ filter.  a=1 and b=1 and c=1 and d =1 are pushed down" in {
    val plan = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d=1").queryExecution.sparkPlan
    plan.nodeName shouldBe "Filter"
    plan.children should have size 1
  }

  it should "allow filter pushdown if all partition columns have EQ filter except for last partition column has IN filters." +
    " a=1 and b=1 and c=1 and d in (1, 2, 3, 4) are pushed down" in {
    val plan = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d in (1, 2, 3, 4)").queryExecution.sparkPlan
    plan.nodeName shouldBe "Filter"
    plan.children should have size 1
  }

  it should "allow filter pushdown if partition column filters can be pushed down and last cluster column has EQ filter." +
    " a=1 and b=1 and c=1 and d in (1, 2, 3, 4) and e=1 are pushed down" in {
    val plan = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d in (1, 2, 3, 4) and e=1").queryExecution.sparkPlan
    plan.nodeName shouldBe "Filter"
    plan.children should have size 1
  }

  it should "allow filter pushdown if partition column filters can be pushed down and last cluster column has range filter." +
    " a=1 and b=1 and c=1 and d in (1, 2, 3, 4) and e=1 and f>1 are pushed down" in {
    val plan = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d in (1, 2, 3, 4) and e=1 and f>1").queryExecution.sparkPlan
    plan.nodeName shouldBe "Filter"
    plan.children should have size 1
  }

  it should "not allow filter pushdown if partition column filters can be pushed down and none-last cluster column has IN filter." +
    " a=1 and b=1 and c=1 and d=3 and e=1 are pushed down" in {
    val result = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and e=1 and f in (1, 2, 3)").collect()
  }

  it should "not allow filter pushdown if partition column filters can be pushed down and none-last cluster column has IN filter." +
    " a=1 and b=1 and c=1 and d=3 are pushed down" in {
    val result = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and h in (1, 2, 3)").collect()
  }

  it should "not allow filter pushdown if partition column filters can be pushed down and none-last cluster column has IN filter." +
    " a=1 and b=1 and c=1 and d=3 and e in (1, 2, 3) are pushed down" in {
    val result = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and e in (1, 2, 3)").collect()
  }

  it should "allow filter pushdown if there is only cluster column filters." +
    " e=1 and f=3 are pushed down" in {
    val plan = sqlContext.sql("select * from test_pd where e=1 and f=3").queryExecution.sparkPlan
    plan.nodeName shouldBe "Filter"
    plan.children should have size 1
  }

  it should "not allow filter pushdown if previous cluster column has None-EQ filter." +
    " a=1 and b=1 and c=1 and d=3 and e=1 and f>2 are pushed down" in {
    val result = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d =3 and e=1 and f>2 and g=3").collect()
  }

  it should "not allow filter pushdown if previous cluster column has None-EQ filter." +
    " a=1 and b=1 and c=1 and d=3 and e>1 are pushed down" in {
    val result = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d =3 and e>1 and f>2 and g=3").collect()
  }

  it should "allow filter pushdown if partition column filters can be pushed down and only last cluster column has IN filter." +
    " a=1 and b=1 and c=1 and d=3 and e=1 and f=1 and g=2 and h in (1, 2, 3) are pushed down" in {
    val plan = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and e=1 and f=1 and g=2 and h in (1, 2, 3)").queryExecution.sparkPlan
    plan.nodeName shouldBe "Filter"
    plan.children should have size 1
  }

  it should "not allow filter pushdown if previous cluster column has None-EQ filter." +
    " a=1 and b=1 and c=1 and d=3 and e=1 and  f>1 are pushed down" in {
    val result = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and e=1 and f>1 and h in (1, 2, 3)").collect()
  }

  it should "allow filter pushdown if there is EQ indexed column filter. restriction on partition columns and cluster columns " +
    " can be loosed up. f=5 and i=4 are pushed down" in {
    val plan = sqlContext.sql("select * from test_pd where f=5 and i=4").queryExecution.sparkPlan
    plan.nodeName shouldBe "Filter"
    plan.children should have size 1
  }

  it should "not allow to push down cluster column IN filter if there is EQ indexed column filter. " +
    " i=4 are pushed down" in {
    val result = sqlContext.sql("select * from test_pd where c=3 and g in (1,2,3) and i=4").collect()
  }

  it should "not allow to push down partition column IN filter if there is EQ indexed column filter. " +
    " i=4 are pushed down" in {
    val result = sqlContext.sql("select * from test_pd where c=3 and d in (1,2,3) and i=4").collect()
  }

  it should "not allow to push down indexed column filters if all indexed column filters are none-EQ filters. " in {
    val result = sqlContext.sql("select * from test_pd where c=3 and i>4").collect()
  }

  it should "allow filter pushdown if there is EQ indexed column filters. " +
    " i=4 and j>5 are pushed down" in {
    val result = sqlContext.sql("select * from test_pd where c=3 and i=4 and j>5").collect()
  }

  it should "not allow to push down none-primary column IN filter. " +
    " e=3 is pushed down" in {
    val result = sqlContext.sql("select * from test_pd where e=3 and j in (1,2,3)").collect()
  }

  it should "not allow to push down none-primary column filters if there are no indexed column filters. " +
    " a=1 and b=1 and c=1 and d=3 are pushed down" in {
    val result = sqlContext.sql("select * from test_pd where a=1 and b=1 and c=1 and d=3 and k=4").collect()
  }

}
