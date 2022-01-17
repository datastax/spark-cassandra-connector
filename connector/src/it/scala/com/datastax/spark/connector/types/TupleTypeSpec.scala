package com.datastax.spark.connector.types

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.scalatest.matchers.{MatchResult, Matcher}

case class Ingredients(id: Int, ingredient: (String, Array[Byte]))

case class Recipes(id: Int, ingredients: ((String, Array[Byte]), (String, Array[Byte])))

class TupleTypeSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(sparkConf)

  val IngredientsTable = "ingredients"
  val RecipesTable = "recipes"

  def makeTupleTables(session: CqlSession): Unit = {
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.$IngredientsTable
         |(id int PRIMARY KEY, ingredient tuple<text, blob>);""".stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.$RecipesTable
         |(id int PRIMARY KEY, ingredients tuple<tuple<text, blob>, tuple<text, blob>>)""".stripMargin)
  }

  override def beforeClass {
    conn.withSessionDo { session =>
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
          .stripMargin)
      makeTupleTables(session)
    }
  }

  private val beTheSameIngredientAs = (expected: (String, Array[Byte])) =>
    Matcher { (left: (String, Array[Byte])) =>
      MatchResult(
        (left._1 equals expected._1) && (left._2 sameElements expected._2),
        s"$left equals $expected",
        s"$left does not equal $expected"
      )
    }

  "SparkSql" should "write tuples with BLOB elements" in {
    val expected = ("fish", "><>".getBytes)
    spark.createDataFrame(Seq(Ingredients(1, expected)))
      .write
      .cassandraFormat(IngredientsTable, ks)
      .mode("append")
      .save()
    val row = spark.sparkContext
      .cassandraTable[Ingredients](ks, IngredientsTable)
      .collect()
      .head
    row.ingredient should beTheSameIngredientAs(expected)
  }

  it should "write nested tuples" in {
    val expected = (("fish", "><>".getBytes), ("poisson", "»<>".getBytes))
    spark.createDataFrame(Seq(Recipes(1, expected)))
      .write
      .cassandraFormat(RecipesTable, ks)
      .mode("append")
      .save()
    val row = spark.sparkContext
      .cassandraTable[Recipes](ks, RecipesTable)
      .collect()
      .head
    row.ingredients._1 should beTheSameIngredientAs(expected._1)
    row.ingredients._2 should beTheSameIngredientAs(expected._2)
  }

}
