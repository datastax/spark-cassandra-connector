package com.datastax.spark.connector.mapper

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder
import com.datastax.spark.connector.types._
import org.scalatest.{Matchers, WordSpec}

class TableDescriptorSpec extends WordSpec with Matchers {

  "A TableDef#cql method" should {
    "produce valid CQL" when {
      "it contains no clustering columns" in {
        val col1 = ColumnDescriptor.partitionKey("c1", IntType)
        val col2 = ColumnDescriptor.regularColumn("c2", VarCharType)
        val table = TableDescriptor("keyspace", "table", Seq(col1, col2))
        table.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  PRIMARY KEY (("c1"))
            |)""".stripMargin
        )
      }

      "it contains clustering columns" in {
        val col1 = ColumnDescriptor.partitionKey("c1", IntType)
        val col2 = ColumnDescriptor.clusteringColumnAsc("c2", VarCharType)
        val col3 = ColumnDescriptor.regularColumn("c3", VarCharType)
        val table = TableDescriptor("keyspace", "table", Seq(col1, col2, col3))
        table.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" varchar,
            |  "c3" varchar,
            |  PRIMARY KEY (("c1"), "c2")
            |)""".stripMargin
        )
      }

      "it contains compound partition key and multiple clustering columns" in {
        val col1 = ColumnDescriptor.partitionKey("c1", IntType)
        val col2 = ColumnDescriptor.partitionKey("c2", VarCharType)
        val col3 = ColumnDescriptor.clusteringColumnAsc("c3", VarCharType)
        val col4 = ColumnDescriptor.clusteringColumnAsc("c4", VarCharType)
        val col5 = ColumnDescriptor.regularColumn("c5", VarCharType)
        val table = TableDescriptor("keyspace", "table", Seq(col1, col2, col3, col4, col5))
        table.cql should be(
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
        val col1 = ColumnDescriptor("c1", IntType, true)
        val col2 = ColumnDescriptor("c2", MapType(BigIntType, VarCharType), false)
        val table = TableDescriptor("keyspace", "table", Seq(col1, col2))
        table.cql should be(
          """CREATE TABLE "keyspace"."table" (
            |  "c1" int,
            |  "c2" map<bigint, varchar>,
            |  PRIMARY KEY (("c1"))
            |)""".stripMargin
        )
      }

      "it contains compound partition key, if not exists, multiple clustering columns with sorting and options" in {
        val col1 = ColumnDescriptor.partitionKey("c1", IntType)
        val col2 = ColumnDescriptor.partitionKey("c2", VarCharType)
        val col3 = ColumnDescriptor.clusteringColumnAsc("c3", VarCharType)
        val col4 = ColumnDescriptor.clusteringColumnDesc("c4", VarCharType)
        val col5 = ColumnDescriptor.regularColumn("c5", VarCharType)
        //
        val tableSortOnly = TableDescriptor("keyspace", "table", Seq(col1, col2, col3, col4, col5), ifNotExists = true)
        tableSortOnly.cql should be(
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
        val tableSortAndOptions =
          TableDescriptor("keyspace", "table",
            Seq(col1, col2, col3, col4, col5),
            ifNotExists = true,
            options = Map("dclocal_read_repair_chance"-> "0.1" ))
        tableSortAndOptions.cql should be(
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
        val tableOptionsOnly =
          TableDescriptor("keyspace", "table",
            Seq(col1, col2, col3, col5),
            ifNotExists = true,
            options = Map("dclocal_read_repair_chance"-> "0.1"))
        tableOptionsOnly.cql should be(
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
