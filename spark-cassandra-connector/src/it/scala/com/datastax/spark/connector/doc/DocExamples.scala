package com.datastax.spark.connector.doc

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.types.CassandraOption
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.WriteConf

import scala.concurrent.Future

class DocExamples extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultConf)

  val conn = CassandraConnector(defaultConf)
  override val ks = "doc_example"

  conn.withSessionDo { session =>
    createKeyspace(session)

    awaitAll(
      Future {
        session.execute(
          s"""CREATE TABLE doc_example.tab1 (key INT, col_1 INT, col_2 INT,
             |PRIMARY KEY (key))""".stripMargin)
        session.execute(
          s"""INSERT INTO doc_example.tab1 (key, col_1, col_2) VALUES (1, null, 1)
             |
           """.stripMargin)
      },
      Future {
        session.execute(
          s"""CREATE TABLE doc_example.tab2 (key INT, col_1 INT, col_2 INT,
             |PRIMARY KEY (key))""".stripMargin)
        session.execute(
          s"""INSERT INTO doc_example.tab2 (key, col_1, col_2) VALUES (1, 5, null)
             |
           """.stripMargin)
      })
  }

  "Docs" should "demonstrate copying a table without deletes" in {
    sc.cassandraTable[(Int, CassandraOption[Int], CassandraOption[Int])](ks, "tab1")
      .saveToCassandra(ks, "tab2")
    sc.cassandraTable[(Int, Int, Int)](ks, "tab2").collect should contain((1, 5, 1))
  }

  it should "demonstrate only deleting some records" in {
    sc.parallelize(1 to 6).map(x => (x, x, x)).saveToCassandra(ks, "tab1")
    sc.parallelize(1 to 6).map(x => x match {
      case x if (x >= 5) => (x, CassandraOption.Null, CassandraOption.Unset)
      case x if (x <= 2) => (x, CassandraOption.Unset, CassandraOption.Null)
      case x => (x, CassandraOption(-1), CassandraOption(-1))
    }).saveToCassandra(ks, "tab1")


    val results = sc.cassandraTable[(Int, Option[Int], Option[Int])](ks, "tab1").collect
    results should contain theSameElementsAs Seq(
      (1, Some(1), None),
      (2, Some(2), None),
      (3, Some(-1), Some(-1)),
      (4, Some(-1), Some(-1)),
      (5, None, Some(5)),
      (6, None, Some(6)))
  }

  it should "demonstrate converting Options to CassandraOptions" in {
    import com.datastax.spark.connector.types.CassandraOption
    //Setup original data (1, 1, 1) --> (6, 6, 6)
    sc.parallelize(1 to 6).map(x => (x, x, x)).saveToCassandra(ks, "tab1")

    //Setup options Rdd (1, -1, None) (2, -1, None) (3, -1, None)
    val optRdd = sc.parallelize(1 to 6)
      .map(x => (x, None, None))
      .map { case (x: Int, y: Option[Int], z: Option[Int]) =>
        (x, CassandraOption.deleteIfNone(y), CassandraOption.unsetIfNone(z))
      }.saveToCassandra(ks, "tab1")

    val results = sc.cassandraTable[(Int, Option[Int], Option[Int])](ks, "tab1").collect

    results should contain theSameElementsAs Seq(
      (1, None, Some(1)),
      (2, None, Some(2)),
      (3, None, Some(3)),
      (4, None, Some(4)),
      (5, None, Some(5)),
      (6, None, Some(6))
    )

  }

  it should "show using a write conf to ignore nulls" in {
    //Setup original data (1, 1, 1) --> (6, 6, 6)
    sc.parallelize(1 to 6).map(x => (x, x, x)).saveToCassandra(ks, "tab1")

    val ignoreNullsWriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true)
    //These writes will do not delete because we are ignoring nulls
    val optRdd = sc.parallelize(1 to 6)
      .map(x => (x, None, None))
      .saveToCassandra(ks, "tab1", writeConf = ignoreNullsWriteConf)

    val results = sc.cassandraTable[(Int, Int, Int)](ks, "tab1").collect

    results should contain theSameElementsAs Seq(
      (1, 1, 1),
      (2, 2, 2),
      (3, 3, 3),
      (4, 4, 4),
      (5, 5, 5),
      (6, 6, 6)
    )

  }
}
