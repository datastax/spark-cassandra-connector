package com.datastax.spark.connector.writer

import com.datastax.spark.connector.{ColumnIndex, ColumnName, ColumnRef}
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper

import scala.collection.Seq
import scala.collection.JavaConversions._

/** A `RowWriter` suitable for saving objects mappable by a [[com.datastax.spark.connector.mapper.ColumnMapper ColumnMapper]].
  * Can save case class objects, java beans and tuples. */
class DefaultRowWriter[T : ColumnMapper](table: TableDef, selectedColumns: Seq[String], aliasToColumName: Predef.Map[String, String])
  extends RowWriter[T] {

  // do not save reference to ColumnMapper in a field, because it is non Serializable
  private val cls = implicitly[ColumnMapper[T]].classTag.runtimeClass.asInstanceOf[Class[T]]
  private val columnMap = implicitly[ColumnMapper[T]].columnMap(table, aliasToColumName)
  private val selectedColumnsSet = selectedColumns.toSet
  private val selectedColumnsIndexed = selectedColumns.toIndexedSeq

  private def checkMissingProperties(requestedPropertyNames: Seq[String]) {
    val availablePropertyNames = PropertyExtractor.availablePropertyNames(cls, requestedPropertyNames)
    val missingColumns = requestedPropertyNames.toSet -- availablePropertyNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"One or more properties not found in RDD data: ${missingColumns.mkString(", ")}")
  }

  private def checkUndefinedColumns(mappedColumns: Seq[String]) {
    val undefinedColumns = selectedColumns.toSet -- mappedColumns.toSet
    if (undefinedColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Missing required columns in RDD data: ${undefinedColumns.mkString(", ")}"
      )
  }

  private def columnNameByRef(columnRef: ColumnRef): Option[String] = {
    columnRef match {
      case ColumnName(name, _) if selectedColumnsSet.contains(name) => Some(name)
      case ColumnIndex(index) if index < selectedColumns.size => Some(selectedColumnsIndexed(index))
      case _ => None
    }
  }

  val (propertyNames, columnNames) = {
    val propertyToColumnName = columnMap.getters.mapValues(columnNameByRef).toSeq
    val selectedPropertyColumnPairs =
      for ((propertyName, Some(columnName)) <- propertyToColumnName if selectedColumnsSet.contains(columnName))
      yield (propertyName, columnName)
    selectedPropertyColumnPairs.unzip
  }

  checkMissingProperties(propertyNames)
  checkUndefinedColumns(columnNames)

  private val columnNameToPropertyName = (columnNames zip propertyNames).toMap
  private val extractor = new PropertyExtractor(cls, propertyNames)

  override def readColumnValues(data: T, buffer: Array[Any]) = {
    for ((c, i) <- columnNames.zipWithIndex) {
      val propertyName = columnNameToPropertyName(c)
      val value = extractor.extractProperty(data, propertyName)
      buffer(i) = value
    }
  }
}

object DefaultRowWriter {

  def factory[T : ColumnMapper] = new RowWriterFactory[T] {
    override def rowWriter(tableDef: TableDef, columnNames: Seq[String], aliasToColumnName: Predef.Map[String, String]) = {
      new DefaultRowWriter[T](tableDef, columnNames, aliasToColumnName)
    }
  }
}

