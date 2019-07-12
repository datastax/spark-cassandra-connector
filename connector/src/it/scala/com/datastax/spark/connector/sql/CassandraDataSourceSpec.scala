package com.datastax.spark.connector.sql

import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.cassandra.{AnalyzedPredicates, CassandraPredicateRules, CassandraSourceOptions, CassandraSourceRelation, TableRef}
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterEach
import com.datastax.spark.connector._
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd.{CassandraJoinRDD, CassandraTableScanRDD}
import com.datastax.spark.connector.util.Logging
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSourceRelation.InClauseToJoinWithTableConversionThreshold
import org.apache.spark.sql.execution.RowDataSourceScanExec

import scala.concurrent.Future

class CassandraDataSourceSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with Logging with BeforeAndAfterEach {

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
        session.execute(s"CREATE TABLE $ks.test_rowwriter (a INT PRIMARY KEY, b INT)")
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

  override def beforeClass {
    createTempTable(ks, "test1", "tmpTable")
    createTempTable(ks, "df_test2", "tmpDf_test2")
  }

  override def afterClass {
    sparkSession.sql("DROP VIEW tmpTable")
    sparkSession.sql("DROP VIEW tmpDf_test2")
  }

  private def withConfig(params: (String, Any)*)(testFun: => Unit): Unit = {
    val originalValues = params.map { case (k, _) => (k, sc.getLocalProperty(k)) }
    params.foreach { case (k, v) => sc.setLocalProperty(k, v.toString) }
    try {
      testFun
    } finally {
      originalValues.foreach { case (k, v) => sc.setLocalProperty(k, v) }
    }
  }

  private def withConfig(key: String, value: Any)(testFun: => Unit): Unit = withConfig((key, value)){testFun}

  def createTempTable(keyspace: String, table: String, tmpTable: String) = {
    sparkSession.sql(
      s"""
        |CREATE TEMPORARY TABLE $tmpTable
        |USING org.apache.spark.sql.cassandra
        |OPTIONS (
        | table "$table",
        | keyspace "$keyspace",
        | pushdown "$pushDown",
        | confirm.truncate "true")
      """.stripMargin.replaceAll("\n", " "))
  }

  def cassandraTable(tableRef: TableRef) : DataFrame = {
    sparkSession.baseRelationToDataFrame(CassandraSourceRelation(tableRef, sparkSession.sqlContext, new CassandraSourceOptions(), None))
  }

  "Cassandra Source Relation" should "allow to select all rows" in {
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
      .options(Map("table" -> "test_insert2", "keyspace" -> ks, "confirm.truncate" -> "true"))
      .save()
    createTempTable(ks, "test_insert2", "insertTable2")
    sparkSession.sql("SELECT * FROM insertTable2").collect() should have length 1
    sparkSession.sql("DROP VIEW insertTable2")
  }

  // This test is just to make sure at runtime the implicit for RDD[Row] can be found
  it should "implicitly generate a rowWriter from it's RDD form" in {
    sparkSession.sql("SELECT a, b from tmpTable").rdd.saveToCassandra(ks, "test_rowwriter")
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
      .options(Map("table" -> "df_test", "keyspace" -> ks, "confirm.truncate" -> "true"))
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
      .options(Map("table" -> "df_test2", "keyspace" -> ks, "confirm.truncate" -> "true"))
      .save()
    cassandraTable(TableRef("df_test2", ks)).collect() should have length 1
  }

  it should "throws exception during overwriting a table when confirm.truncate is false" in {
    val test_df = TestPartialColumns(1400820884, "Firefox", 123242)
    val ss = sparkSession
    import ss.implicits._

    val df = sc.parallelize(Seq(test_df)).toDF

    val message = intercept[UnsupportedOperationException] {
      df.write
        .format("org.apache.spark.sql.cassandra")
        .mode(Overwrite)
        .options(Map("table" -> "df_test2", "keyspace" -> ks, "confirm.truncate" -> "false"))
        .save()
    }.getMessage

    assert(
      message.contains("You are attempting to use overwrite mode"),
      "Exception should be thrown when  attempting to overwrite a table if confirm.truncate is false")

  }

  it should "apply user custom predicates which erase basic pushdowns" in withConfig(
      CassandraSourceRelation.AdditionalCassandraPushDownRulesParam.name,
      "com.datastax.spark.connector.sql.PushdownNothing") {

    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1"))
      .load().filter("a=1 and b=2 and c=1 and e=1")

    val qp = df.queryExecution.executedPlan.toString
    qp should include ("Filter (") // Should have a Spark Filter Step
  }

  it should "apply user custom predicate rules in the order they are specified" in withConfig(
      CassandraSourceRelation.AdditionalCassandraPushDownRulesParam.name,
      "com.datastax.spark.connector.sql.PushdownNothing,com.datastax.spark.connector.sql.PushdownEverything,com.datastax.spark.connector.sql.PushdownEqualsOnly") {

    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1"))
      .load()
      .filter("a=1 and b=2 and c=1 and e=1")
      .select("a", "b", "c", "e")

    def getSourceRDD(rdd: RDD[_]): RDD[_] = {
      if (rdd.dependencies.nonEmpty)
        getSourceRDD(rdd.dependencies.head.rdd)
      else
        rdd
    }

    val cassandraTableScanRDD = getSourceRDD(df.queryExecution
      .executedPlan
      .collectLeaves().head //Get Source
      .asInstanceOf[RowDataSourceScanExec]
      .rdd).asInstanceOf[CassandraTableScanRDD[_]]

    val pushedWhere = cassandraTableScanRDD.where
    val predicates = pushedWhere.predicates.head.split("AND").map(_.trim)
    val values = pushedWhere.values
    val pushedPredicates = predicates.zip(values)
    pushedPredicates should contain allOf(
      ("\"a\" = ?", 1),
      ("\"b\" = ?", 2),
      ("\"c\" = ?", 1),
      ("\"e\" = ?", 1))
  }

  it should "pass through local conf properties" in withConfig(
      CassandraSourceRelation.AdditionalCassandraPushDownRulesParam.name,
      "com.datastax.spark.connector.sql.PushdownUsesConf") {

    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1", PushdownUsesConf.testKey -> "Don't Remove"))
      .load().filter("g=1 and h=1")

    df.queryExecution.executedPlan // Will throw an exception if local key is not set

    val df2 = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> ks, "table" -> "test1"))
      .load().filter("g=1 and h=1")

    intercept[IllegalAccessException] {
      df2.explain() //
    }
  }

  private def joinConversionLowThreshold(testFun: => Unit): Unit =
    withConfig(InClauseToJoinWithTableConversionThreshold.name, 1) { testFun }

  private def flattenRddDependencies(rdds: Seq[RDD[_]], rdd: RDD[_]): Seq[RDD[_]] =
    if (rdd.dependencies.isEmpty) rdds :+ rdd else rdd.dependencies.flatMap(d => flattenRddDependencies(rdds :+ rdd, d.rdd))

  private def assertOnCassandraJoinRddPresence(rdd: RDD[_]): Unit = {
    if (pushDown)
      withClue("Given RDD does not contain CassandraJoinRDD in it's predecessors.") {
        flattenRddDependencies(Seq[RDD[_]](), rdd).find(_.isInstanceOf[CassandraJoinRDD[_, _]]) shouldBe defined
     }
    else
      assertOnAbsenceOfCassandraJoinRdd(rdd)
  }

  private def assertOnAbsenceOfCassandraJoinRdd(rdd: RDD[_]): Unit =
    withClue("Given RDD contains CassandraJoinRDD in it's predecessors.") {
      flattenRddDependencies(Seq[RDD[_]](), rdd).find(_.isInstanceOf[CassandraJoinRDD[_, _]]) should not be defined
    }

  it should "convert to joinWithCassandra for 'IN' clause spanned on simple partition key " in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT * FROM tmpDf_test2 WHERE customer_id IN (1,2)")
    assertOnCassandraJoinRddPresence(df.rdd)
  }

  it should "convert to joinWithCassandra for 'IN' clause for composite partition key " in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT a,b FROM tmpTable WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2)")
    assertOnCassandraJoinRddPresence(df.rdd)
    df.collect() should have size 8
  }

  it should "convert to joinWithCassandra for 'IN' clause for some columns of composite partition key " in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE a IN (1,2) AND b = 2 AND c IN (1,2,3)")
    assertOnCassandraJoinRddPresence(df.rdd)
    df.collect() should have size 4
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition key and some clustering columns" in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2) AND d IN (1) AND e IN (1)")
    assertOnCassandraJoinRddPresence(df.rdd)
    df.collect() should have size 2
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition key and all clustering columns" in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2) AND d IN (1) AND e IN (1) AND f IN (1,3)")
    assertOnCassandraJoinRddPresence(df.rdd)
    df.collect() should have size 1
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition and some clustering columns and range" in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2) AND d IN (1) AND e IN (1) AND f < 2")
    assertOnCassandraJoinRddPresence(df.rdd)
    val rows = df.collect()
    rows should have size 1
    rows.head.size should be(8)
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition columns and respect required columns" in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT a,b,c FROM tmpTable WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2)")
    assertOnCassandraJoinRddPresence(df.rdd)
    df.collect().head.size should be(3)
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition columns and retrieve all columns" in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2)")
    assertOnCassandraJoinRddPresence(df.rdd)
    df.collect().head.size should be(8)
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition key and clustering columns regardless the predicate order" in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE e IN (2) AND b IN (1,2) AND a IN (1,2) AND d IN (2) AND f IN (2) AND c IN (1)")
    assertOnCassandraJoinRddPresence(df.rdd)
    val rows = df.collect()
    rows should have size 1
    rows.head.mkString(",") should be("1,2,1,2,2,2,2,2")
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition key and clustering columns with equal predicates" in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE e = 2 AND b IN (1,2) AND a IN (1,2) AND d = 2 AND f IN (2) AND c = 1")
    assertOnCassandraJoinRddPresence(df.rdd)
    val rows = df.collect()
    rows should have size 1
    rows.head.mkString(",") should be("1,2,1,2,2,2,2,2")
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition columns and respect required columns order" in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT b,a FROM tmpTable WHERE a IN (1) AND b IN (2) AND c IN (1,9)")
    assertOnCassandraJoinRddPresence(df.rdd)
    val rows = df.collect()
    rows should have size 4
    rows.foreach(row => row.mkString(",") should be("2,1"))
  }

  it should "convert to joinWithCassandra for 'IN' clause for partition columns and allow count" in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT count(1) FROM tmpTable WHERE a IN (1) AND b IN (2) AND c IN (1,9)")
    assertOnCassandraJoinRddPresence(df.rdd)
    val rows = df.collect()
    rows should have size 1
    rows.head.getLong(0) should be(4)
  }

  it should "not convert to joinWithCassandra when some partition columns have no predicate" in joinConversionLowThreshold {
    val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE a IN (1) AND b IN (2,3,4,5)")
    assertOnAbsenceOfCassandraJoinRdd(df.rdd)
  }

  it should "not convert to joinWithCassandra if 'IN' value sets cumulative size does not exceed configurable threshold" in
      withConfig(InClauseToJoinWithTableConversionThreshold.name, 9) {
    val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE a IN (1,2) AND b IN (2,1) AND c IN (3,1)")
    assertOnAbsenceOfCassandraJoinRdd(df.rdd)
  }

  it should "not convert to joinWithCassandra for 'IN' clause if threshold is 0" in
      withConfig((InClauseToJoinWithTableConversionThreshold.name, 0)) {
        val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE a IN (1) AND b IN (2) AND c IN (1,9)")
        assertOnAbsenceOfCassandraJoinRdd(df.rdd)
  }

  it should "keep default parallelism when converting IN clause to joinWithCassandra" in joinConversionLowThreshold {
    if (pushDown) {
      val df = sparkSession.sql(s"SELECT * FROM tmpTable WHERE a IN (1,2) AND b IN (1,2) AND c IN (1,2)")
      assertOnCassandraJoinRddPresence(df.rdd)
      df.rdd.partitions.length should be (df.sparkSession.sparkContext.defaultParallelism)
    }
  }
}

case class Test(epoch: Long, uri: String, browser: String, customer_id: Int)

case class TestPartialColumns(epoch: Long, browser: String, customer_id: Int)

object PushdownEverything extends CassandraPredicateRules {
  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf): AnalyzedPredicates = {

    AnalyzedPredicates(predicates.handledByCassandra ++ predicates.handledBySpark, Set.empty)
  }
}

object PushdownNothing extends CassandraPredicateRules {
  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf): AnalyzedPredicates = {

    AnalyzedPredicates(Set.empty, predicates.handledByCassandra ++ predicates.handledBySpark)
  }
}

object PushdownEqualsOnly extends CassandraPredicateRules {
  override def apply(
    predicates: AnalyzedPredicates,
    tableDef: TableDef,
    sparkConf: SparkConf): AnalyzedPredicates = {

    val eqFilters = (predicates.handledByCassandra ++ predicates.handledBySpark).collect {
      case x: EqualTo => x: Filter
    }
    AnalyzedPredicates(
      eqFilters,
      (predicates.handledBySpark ++ predicates.handledByCassandra) -- eqFilters)
  }
}

/**
  * Throws an exception if test key is not set
  */
object PushdownUsesConf extends CassandraPredicateRules {
  val testKey = "testkey"
  val notSet = "notset"
  override def apply(
                      predicates: AnalyzedPredicates,
                      tableDef: TableDef,
                      conf: SparkConf): AnalyzedPredicates = {
    if (conf.contains(testKey)){
      predicates
    } else {
      throw new IllegalAccessException(s"Conf did not contain $testKey")
    }
  }
}
