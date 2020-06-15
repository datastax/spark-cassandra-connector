package com.datastax.spark.connector.cql

import com.datastax.spark.connector.types._
import org.scalatest.{Matchers, WordSpec}

class TableDefSpec extends WordSpec with Matchers {

  "A TableDef#cql method" should {
    "produce valid CQL" when {
      "it contains no clustering columns" in {
        val column1 = DefaultColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = DefaultColumnDef("c2", RegularColumn, VarCharType)
        val tableDef = DefaultTableDef("keyspace", "table", Seq(column1), Seq.empty, Seq(column2))
        tableDef.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  PRIMARY KEY (("c1"))
            |)""".stripMargin
        )
      }

      "it contains clustering columns" in {
        val column1 = DefaultColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = DefaultColumnDef("c2", ClusteringColumn(0), VarCharType)
        val column3 = DefaultColumnDef("c3", RegularColumn, VarCharType)
        val tableDef = DefaultTableDef("keyspace", "table", Seq(column1), Seq(column2), Seq(column3))
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
        val column1 = DefaultColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = DefaultColumnDef("c2", PartitionKeyColumn, VarCharType)
        val column3 = DefaultColumnDef("c3", ClusteringColumn(0), VarCharType)
        val column4 = DefaultColumnDef("c4", ClusteringColumn(1), VarCharType)
        val column5 = DefaultColumnDef("c5", RegularColumn, VarCharType)
        val tableDef = DefaultTableDef("keyspace", "table", Seq(column1, column2), Seq(column3, column4), Seq(column5))
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
        val column1 = DefaultColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = DefaultColumnDef("c2", RegularColumn, MapType(BigIntType, VarCharType))
        val tableDef = DefaultTableDef("keyspace", "table", Seq(column1), Seq.empty, Seq(column2))
        tableDef.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" map<bigint, varchar>,
            |  PRIMARY KEY (("c1"))
            |)""".stripMargin
        )
      }

      "it contains compound partition key, if not exists, multiple clustering columns with sorting and options" in {
        val column1 = DefaultColumnDef("c1", PartitionKeyColumn, IntType)
        val column2 = DefaultColumnDef("c2", PartitionKeyColumn, VarCharType)
        val column3 = DefaultColumnDef("c3", ClusteringColumn(0), VarCharType)
        val column4 = DefaultColumnDef("c4", ClusteringColumn(1, ClusteringColumn.Descending), VarCharType)
        val column5 = DefaultColumnDef("c5", RegularColumn, VarCharType)
        //
        val tableDefSortOnly = DefaultTableDef("keyspace", "table", Seq(column1, column2), Seq(column3, column4),
          Seq(column5), ifNotExists = true)
        tableDefSortOnly.cql should be(
          """CREATE TABLE IF NOT EXISTS "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  "c4" varchar,
            |  "c5" varchar,
            |  PRIMARY KEY (("c1", "c2"), "c3", "c4")
            |) WITH CLUSTERING ORDER BY ("c3" ASC, "c4" DESC)""".stripMargin
        )
        //
        val tableDefSortAndOptions = DefaultTableDef("keyspace", "table", Seq(column1, column2),
          Seq(column3, column4), Seq(column5), ifNotExists = true,
          tableOptions = Map("dclocal_read_repair_chance"-> "0.1" ))
        tableDefSortAndOptions.cql should be(
          """CREATE TABLE IF NOT EXISTS "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  "c4" varchar,
            |  "c5" varchar,
            |  PRIMARY KEY (("c1", "c2"), "c3", "c4")
            |) WITH CLUSTERING ORDER BY ("c3" ASC, "c4" DESC)
            |  AND dclocal_read_repair_chance = 0.1""".stripMargin
        )
        //
        val tableDefOptionsOnly = DefaultTableDef("keyspace", "table", Seq(column1, column2),
          Seq(column3), Seq(column5), ifNotExists = true,
          tableOptions = Map("dclocal_read_repair_chance"-> "0.1"))
        tableDefOptionsOnly.cql should be(
          """CREATE TABLE IF NOT EXISTS "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  "c5" varchar,
            |  PRIMARY KEY (("c1", "c2"), "c3")
            |) WITH dclocal_read_repair_chance = 0.1""".stripMargin
        )
      }
    }
  }
}
