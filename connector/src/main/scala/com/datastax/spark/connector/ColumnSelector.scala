package com.datastax.spark.connector

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata

import scala.language.implicitConversions
import com.datastax.spark.connector.cql.{ColumnDef, StructDef, TableDef}

sealed trait ColumnSelector {
  def aliases: Map[String, String]
  def selectFrom(table: TableDef): IndexedSeq[ColumnRef]
  def selectFrom(table: TableMetadata): IndexedSeq[ColumnRef]
}

case object AllColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
  override def selectFrom(table: TableDef) =
    table.columns.map(_.ref)
  override def selectFrom(table: TableMetadata) =
    TableDef.columns(table).map(ColumnDef.toRef(_))
}

case object PartitionKeyColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
  override def selectFrom(table: TableDef) =
    table.partitionKey.map(_.ref).toIndexedSeq
  override def selectFrom(table: TableMetadata) =
    TableDef.partitionKey(table).map(ColumnDef.toRef(_)).toIndexedSeq
}

case object PrimaryKeyColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
  override def selectFrom(table: TableDef) =
    table.primaryKey.map(_.ref)
  override def selectFrom(table: TableMetadata) =
    TableDef.primaryKey(table).map(ColumnDef.toRef(_))
}

case class SomeColumns(columns: ColumnRef*) extends ColumnSelector {

  private def columnsToCheck():Seq[ColumnRef] = columns flatMap {
    case f: FunctionCallRef => f.requiredColumns //Replaces function calls by their required columns
    case RowCountRef => Seq.empty //Filters RowCountRef from the column list
    case other => Seq(other)
  }

  /** Compute the columns that are not present in the structure. */
  private def missingColumns(table:StructDef, columnsToCheck: Seq[ColumnRef]): Seq[ColumnRef] =
    for (c <- columnsToCheck if !table.columnByName.contains(c.columnName)) yield c

  private def missingColumns(table:TableMetadata, columnsToCheck: Seq[ColumnRef]): Seq[ColumnRef] =
    for (c <- columnsToCheck if !TableDef.containsColumn(c.columnName)(table)) yield c

  override def aliases: Map[String, String] = columns.map {
    case ref => (ref.selectedAs, ref.cqlValueName)
  }.toMap

  override def selectFrom(table: TableDef): IndexedSeq[ColumnRef] = {
    val missing = missingColumns(table, columnsToCheck)
    if (missing.nonEmpty) throw new NoSuchElementException(
      s"Columns not found in table ${table.name}: ${missing.mkString(", ")}")
    columns.toIndexedSeq
  }

  override def selectFrom(table: TableMetadata): IndexedSeq[ColumnRef] = {
    val missing = missingColumns(table, columnsToCheck)
    if (missing.nonEmpty) throw new NoSuchElementException(
      s"Columns not found in table ${TableDef.tableName(table)}: ${missing.mkString(", ")}")
    columns.toIndexedSeq
  }
}

object SomeColumns {
  @deprecated("Use com.datastax.spark.connector.rdd.SomeColumns instead of Seq", "1.0")
  implicit def seqToSomeColumns(columns: Seq[String]): SomeColumns =
    SomeColumns(columns.map(x => x: ColumnRef): _*)
}


