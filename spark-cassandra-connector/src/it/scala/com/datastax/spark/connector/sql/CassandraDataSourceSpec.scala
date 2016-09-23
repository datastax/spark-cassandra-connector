package com.datastax.spark.connector.sql

import scala.concurrent.Future
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.cassandra.{AnalyzedPredicates, CassandraPredicateRules, CassandraSourceRelation, TableRef}
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterEach
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.embedded.YamlTransformations
import com.datastax.spark.connector.util.Logging

class CassandraDataSourceSpec extends SparkCassandraITFlatSpecBase with Logging with BeforeAndAfterEach {
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  override lazy val conn = CassandraConnector(defaultConf)
  conn.withSessionDo { session =>
    createKeyspace(session)

    awaitAll(
      Future {
        session.execute(s"""CREATE TABLE $ks.test1 (a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT, PRIMARY KEY ((a, b, c), d , e, f))""")
        session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 1, 1, 1, 1)""")
        session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 2, 1, 1, 2)""")
        session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 1, 1, 2, 1)""")
        session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 2, 1, 2, 2)""")
        session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 1, 2, 1, 1)""")
        session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 2, 2, 1, 2)""")
        session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 1, 2, 2, 1)""")
        session.execute(s"""INSERT INTO $ks.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 2, 2, 2, 2)""")
      },

      Future {
        session.execute(s"CREATE TABLE $ks.test_insert (a INT PRIMARY KEY, b INT)")
      },

      Future {
        session.execute(s"CREATE TABLE $ks.test_insert1 (a INT PRIMARY KEY, b INT)")
      },

      Future {
        session.execute(s"CREATE TABLE $ks.test_insert2 (a INT PRIMARY KEY, b INT)")
        session.execute(s"INSERT INTO $ks.test_insert2 (a, b) VALUES (3,4)")
        session.execute(s"INSERT INTO $ks.test_insert2 (a, b) VALUES (5,6)")
      },

      Future {
        session.execute(
          s"""
             |CREATE TABLE $ks.df_test(
             |  customer_id int,
             |  uri text,
             |  browser text,
             |  epoch bigint,
             |  PRIMARY KEY (customer_id, epoch, uri)
             |)""".stripMargin.replaceAll("\n", " "))
      },

      Future {
        session.execute(
          s"""
             |CREATE TABLE $ks.df_test2(
             |  customer_id int,
             |  uri text,
             |  browser text,
             |  epoch bigint,
             |  PRIMARY KEY (customer_id, epoch)
             |)""".stripMargin.replaceAll("\n", " "))
      }
    )
  }

  def pushDown: Boolean = true

  override def beforeAll() {
    createTempTable(ks, "test1", "tmpTable")
  }

  override def afterAll() {
    super.afterAll()
    sparkSession.sql("DROP VIEW tmpTable")
  }

  override def afterEach(): Unit ={
     sc.setLocalProperty(CassandraSourceRelation.AdditionalCassandraPushDownRulesParam.name, null)
  }

  def createTempTable(keyspace: String, table: String, tmpTable: String) = {
    sparkSession.sql(
      s"""
        |CREATE TEMPORARY TABLE $tmpTable
        |USING org.apache.spark.sql.cassandra
        |OPTIONS (
        | table "$table",
        | keyspace "$keyspace",
        | pushdown "$pushDown")
      """.stripMargin.replaceAll("\n", " "))
  }

  def cassandraTable(tableRef: TableRef) : DataFrame = {
    sparkSession.baseRelationToDataFrame(CassandraSourceRelation(tableRef, sparkSession.sqlContext))
  }

  it should "allow to select all rows" in {
    val result = cassandraTable(TableRef("test1", ks)).select("a").collect()
    result should have length 8
    result.head should have length 1
  }

  it should "allow to register as a temp table" in {
    cassandraTable(TableRef("test1", ks)).createOrReplaceTempView("test1")
    val temp = sparkSession.sql("SELECT * from test1").select("b").collect()
    temp should have length 8
    temp.head should have length 1
    sparkSession.sql("DROP VIEW test1")
  }

  it should "allow to insert data into a cassandra table" in {
    createTempTable(ks, "test_insert", "insertTable")
    sparkSession.sql("SELECT * FROM insertTable").collect() should have length 0

    sparkSession.sql("INSERT OVERWRITE TABLE insertTable SELECT a, b FROM tmpTable")
    sparkSession.sql("SELECT * FROM insertTable").collect() should have length 1
    sparkSession.sql("DROP VIEW insertTable")
  }

  it should "allow to save data to a cassandra table" in {
    sparkSession.sql("SELECT a, b from tmpTable")
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode(ErrorIfExists)
      .options(Map("table" -> "test_insert1", "keyspace" -> ks))
      .save()

    cassandraTable(TableRef("test_insert1", ks)).collect() should have length 1

    val message = intercept[UnsupportedOperationException] {
      sparkSession.sql("SELECT a, b from tmpTable")
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode(ErrorIfExists)
        .options(Map("table" -> "test_insert1", "keyspace" -> ks))
        .save()
    }.getMessage

    assert(
      message.contains("SaveMode is set to ErrorIfExists and Table"),
      "We should complain if attempting to write to a table with data if save mode is ErrorIfExists.'")
  }

  it should "allow to overwrite a cassandra table" in {
    sparkSession.sql("SELECT a, b from tmpTable")
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode(Overwrite)
      .options(Map("table" -> "test_insert2", "keyspace" -> ks))
      .save()
    createTempTable(ks, "test_insert2", "insertTable2")
    sparkSession.sql("SELECT * FROM insertTable2").collect() should have length 1
    sparkSession.sql("DROP VIEW insertTable2")
  }

  it should "allow to filter a table" in {
    sparkSession.sql("SELECT a, b FROM tmpTable WHERE a=1 and b=2 and c=1 and e=1").collect() should have length 2
  }

  it should "allow to filter a table with a function for a column alias" in {
    sparkSession.sql("SELECT * FROM (SELECT (a + b + c) AS x, d FROM tmpTable) " +
      "AS tmpTable1 WHERE x= 3").collect() should have length 4
  }

  it should "allow to filter a table with alias" in {
    sparkSession.sql("SELECT * FROM (SELECT a AS a1, b AS b1, c AS c1, d AS d1, e AS e1" +
      " FROM tmpTable) AS tmpTable1 WHERE  a1=1 and b1=2 and c1=1 and e1=1 ").collect() should have length 2
  }

  it should "be able to save DF with reversed order columns to a Cassandra table" in {
    val test_df = Test(1400820884, "http://foobar", "Firefox", 123242)

    val ss = sparkSession
    import ss.implicits._
    val df = sc.parallelize(Seq(test_df)).toDF

    df.write
      .format("org.apache.spark.sql.cassandra")
      .mode(Overwrite)
      .options(Map("table" -> "df_test", "keyspace" -> ks))
      .save()
    cassandraTable(TableRef("df_test", ks)).collect() should have length 1
  }

  it should "be able to save DF with partial columns to a Cassandra table" in {
    val test_df = TestPartialColumns(1400820884, "Firefox", 123242)

    val ss = sparkSession
    import ss.implicits._
    val df = sc.parallelize(Seq(test_df)).toDF

    df.write
      .format("org.apache.spark.sql.cassandra")
      .mode(Overwrite)
      .options(Map("table" -> "df_test2", "keyspace" -> ks))
      .save()
    cassandraTable(TableRef("df_test2", ks)).collect() should have length 1
  }

  it should "apply user custom predicates which erase basic pushdowns" in {
    sc.setLocalProperty(
      CassandraSourceRelation.AdditionalCassandraPushDownRulesParam.name,
      "com.datastax.spark.connector.sql.PushdownNothing")

    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1"))
      .load().filter("a=1 and b=2 and c=1 and e=1")

    val qp = df.queryExecution.executedPlan.toString
    qp should include ("Filter (") // Should have a Spark Filter Step
    println(qp)
  }

  it should "apply user custom predicates in the order they are specified" in {
    sc.setLocalProperty(
      CassandraSourceRelation.AdditionalCassandraPushDownRulesParam.name,
      "com.datastax.spark.connector.sql.PushdownNothing,com.datastax.spark.connector.sql.PushdownEverything,com.datastax.spark.connector.sql.PushdownEqualsOnly")

    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1"))
      .load().filter("a=1 and b=2 and c=1 and e=1")

    val qp = df.queryExecution.executedPlan
    println(qp.constraints)
  }
}

case class Test(epoch: Long, uri: String, browser: String, customer_id: Int)

case class TestPartialColumns(epoch: Long, browser: String, customer_id: Int)

object PushdownEverything extends CassandraPredicateRules {
  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef): AnalyzedPredicates = {

    AnalyzedPredicates(predicates.handledByCassandra ++ predicates.handledBySpark, Set.empty)
  }
}

object PushdownNothing extends CassandraPredicateRules {
  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef): AnalyzedPredicates = {

    AnalyzedPredicates(Set.empty, predicates.handledByCassandra ++ predicates.handledBySpark)
  }
}

object PushdownEqualsOnly extends CassandraPredicateRules {
  override def apply(predicates: AnalyzedPredicates, tableDef: TableDef): AnalyzedPredicates = {
    val eqFilters = (predicates.handledByCassandra ++ predicates.handledBySpark).collect {
      case x: EqualTo => x: Filter
    }
    AnalyzedPredicates(
      eqFilters,
      (predicates.handledBySpark ++ predicates.handledByCassandra) -- eqFilters)
  }
}
