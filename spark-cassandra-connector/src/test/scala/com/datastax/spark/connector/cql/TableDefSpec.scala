package com.datastax.spark.connector.cql

import com.datastax.spark.connector.{PartitionKeyColumns, TTL, SomeColumns, AllColumns}
import com.datastax.spark.connector.types._
import org.scalatest.{WordSpec, Matchers}

class TableDefSpec extends WordSpec with Matchers {

  "A TableDef#cql method" should {
    "produce valid CQL" when {
      "it contains no clustering columns" in {
        val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = ColumnDef("c2", RegularColumn, VarCharType)
        val tableDef = TableDef("keyspace", "table", Seq(column1), Seq.empty, Seq(column2))
        tableDef.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  PRIMARY KEY (("c1"))
            |)""".stripMargin
        )
      }

      "it contains clustering columns" in {
        val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = ColumnDef("c2", ClusteringColumn(0), VarCharType)
        val column3 = ColumnDef("c3", RegularColumn, VarCharType)
        val tableDef = TableDef("keyspace", "table", Seq(column1), Seq(column2), Seq(column3))
        tableDef.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  PRIMARY KEY (("c1"), "c2")
            |)""".stripMargin
        )
      }

      "it contains compound partition key and multiple clustering columns" in {
        val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = ColumnDef("c2", PartitionKeyColumn, VarCharType)
        val column3 = ColumnDef("c3", ClusteringColumn(0), VarCharType)
        val column4 = ColumnDef("c4", ClusteringColumn(1), VarCharType)
        val column5 = ColumnDef("c5", RegularColumn, VarCharType)
        val tableDef = TableDef("keyspace", "table", Seq(column1, column2), Seq(column3, column4), Seq(column5))
        tableDef.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  "c4" varchar,
            |  "c5" varchar,
            |  PRIMARY KEY (("c1", "c2"), "c3", "c4")
            |)""".stripMargin
        )
      }

      "it contains a column of a collection type" in {
        val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = ColumnDef("c2", RegularColumn, MapType(BigIntType, VarCharType))
        val tableDef = TableDef("keyspace", "table", Seq(column1), Seq.empty, Seq(column2))
        tableDef.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" map<bigint, varchar>,
            |  PRIMARY KEY (("c1"))
            |)""".stripMargin
        )
      }
    }
  }

  "A TableDef#select method" should {
    val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
    val column2 = ColumnDef("c2", PartitionKeyColumn, VarCharType)
    val column3 = ColumnDef("c3", ClusteringColumn(0), VarCharType)
    val column4 = ColumnDef("c4", ClusteringColumn(1), VarCharType)
    val column5 = ColumnDef("c5", RegularColumn, VarCharType)
    val column6 = ColumnDef("c6", RegularColumn, TimestampType)

    val tableDef = TableDef("keyspace", "table", Seq(column1, column2), Seq(column3, column4), Seq(column5, column6))

    "return all columns" in {
      val columns = tableDef.select(AllColumns)
      columns should equal(tableDef.columns)
    }

    "return partition key columns" in {
      val columns = tableDef.select(PartitionKeyColumns)
      columns should equal(tableDef.partitionKey)
    }

    "return some columns" in {
      val columns = tableDef.select(SomeColumns("c1", "c3", "c5"))
      columns.map(_.columnName) should be equals Seq("c1", "c3", "c5")
    }

    "throw a NoSuchElementException when selected column name is invalid" in {
      a[NoSuchElementException] should be thrownBy {
        tableDef.select(SomeColumns("c1", "c3", "unknown_column"))
      }
    }

    "throw an IllegalArgumentException when selected column with invalid selector, e.g. TTL" in {
      an[IllegalArgumentException] should be thrownBy {
        tableDef.select(SomeColumns("c1", TTL("c3")))
      }
    }
  }
}
