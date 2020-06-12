package com.datastax.spark.connector

import org.scalatest.{Matchers, WordSpec}
import com.datastax.spark.connector.mapper.{ColumnDescriptor, TableDescriptor}
import com.datastax.spark.connector.types.{IntType, TimestampType, VarCharType}

/* TODO: This spec is now only testing the TableDescriptor side of the house... need to add IT for TableDef side */
class ColumnSelectorSpec extends WordSpec with Matchers {
  "A ColumnSelector#selectFrom method" should {
    val column1 = ColumnDescriptor.partitionKey("c1", IntType)
    val column2 = ColumnDescriptor.partitionKey("c2", VarCharType)
    val column3 = ColumnDescriptor.clusteringColumn("c3", VarCharType)
    val column4 = ColumnDescriptor.clusteringColumn("c4", VarCharType)
    val column5 = ColumnDescriptor.regularColumn("c5", VarCharType)
    val column6 = ColumnDescriptor.regularColumn("c6", TimestampType)

    val tableDef = TableDescriptor("keyspace", "table", Seq(column1, column2, column3, column4, column5, column6))

    "return all columns" in {
      val columns = AllColumns.selectFrom(tableDef)
      columns should equal(tableDef.columns.map(_.ref))
    }

    "return partition key columns" in {
      val columns = PartitionKeyColumns.selectFrom(tableDef)
      columns should equal(tableDef.partitionKey.map(_.ref))
    }

    "return some columns" in {
      val columns = SomeColumns("c1", "c3", "c5").selectFrom(tableDef)
      columns.map(_.columnName) should be equals Seq("c1", "c3", "c5")
    }

    "return selections with function calls" in {
      val selection = SomeColumns(
        ColumnName("c1"),
        FunctionCallRef("f", Left(ColumnName("c2"))::Nil)).selectFrom(tableDef)

      selection.map(_.cql) should be equals Seq(""""c1"""", """f("c2")""")
    }

    "throw a NoSuchElementException when selected column name is invalid" in {
      a[NoSuchElementException] should be thrownBy {
        SomeColumns("c1", "c3", "unknown_column").selectFrom(tableDef)
      }
    }

    "throw a NoSuchElementException when a function call has a missing column as an actual parameter" in {
      a[NoSuchElementException] should be thrownBy {
        SomeColumns("c1", FunctionCallRef("f", Left(ColumnName("unknown_column"))::Nil)).selectFrom(tableDef)
      }
    }
  }
}
