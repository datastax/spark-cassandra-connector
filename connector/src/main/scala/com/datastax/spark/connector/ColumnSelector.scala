package com.datastax.spark.connector

import com.datastax.spark.connector.cql.{StructDef, TableDef}

import scala.language.implicitConversions
import com.datastax.spark.connector.mapper.TableDescriptor

sealed trait ColumnSelector {
  def aliases: Map[String, String]
  def selectFrom(table: TableDef): IndexedSeq[ColumnRef]
  def selectFrom(table: TableDescriptor): IndexedSeq[ColumnRef]
}

case object AllColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
  override def selectFrom(table: TableDef) =
    table.columns.map(_.ref)
  override def selectFrom(table: TableDescriptor) =
    table.columns.map(_.ref)
}

case object PartitionKeyColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
  override def selectFrom(table: TableDef) =
    table.partitionKey.map(_.ref).toIndexedSeq
  override def selectFrom(table: TableDescriptor) =
    table.partitionKey.map(_.ref).toIndexedSeq
}

case object PrimaryKeyColumns extends ColumnSelector {
  override def aliases: Map[String, String] = Map.empty.withDefault(x => x)
  override def selectFrom(table: TableDef) =
    table.primaryKey.map(_.ref)
  override def selectFrom(table: TableDescriptor) =
    table.primaryKey.map(_.ref)
}

case class SomeColumns(columns: ColumnRef*) extends ColumnSelector {

  override def aliases: Map[String, String] = columns.map {
    case ref => (ref.selectedAs, ref.cqlValueName)
  }.toMap

  override def selectFrom(table: TableDef): IndexedSeq[ColumnRef] = {
    selectFromStructDef(table)
  }

  override def selectFrom(table: TableDescriptor): IndexedSeq[ColumnRef] = {
    selectFromStructDef(table)
  }

  private def selectFromStructDef(table: StructDef): IndexedSeq[ColumnRef] = {
    val missing = table.missingColumns {
      columns flatMap {
        case f: FunctionCallRef => f.requiredColumns //Replaces function calls by their required columns
        case RowCountRef => Seq.empty //Filters RowCountRef from the column list
        case other => Seq(other)
      }
    }
    if (missing.nonEmpty) throw new NoSuchElementException(
      s"Columns not found in table ${table.name}: ${missing.mkString(", ")}")

    columns.toIndexedSeq
  }
}

object SomeColumns {
  @deprecated("Use com.datastax.spark.connector.rdd.SomeColumns instead of Seq", "1.0")
  implicit def seqToSomeColumns(columns: Seq[String]): SomeColumns =
    SomeColumns(columns.map(x => x: ColumnRef): _*)
}


