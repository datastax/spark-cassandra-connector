package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ProtocolVersion, PreparedStatement}
import com.datastax.spark.connector.{ColumnIndex, ColumnName, ColumnRef}
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.types.TypeConverter

import scala.collection.{Map, Seq}
import scala.collection.JavaConversions._

/** A `RowWriter` suitable for saving objects mappable by a [[com.datastax.spark.connector.mapper.ColumnMapper ColumnMapper]].
  * Can save case class objects, java beans and tuples. */
class DefaultRowWriter[T : ColumnMapper](table: TableDef, selectedColumns: Seq[String])
  extends RowWriter[T] {

  // do not save reference to ColumnMapper in a field, because it is non Serializable
  private val cls = implicitly[ColumnMapper[T]].classTag.runtimeClass.asInstanceOf[Class[T]]
  private val columnMap = implicitly[ColumnMapper[T]].columnMap(table)
  private val selectedColumnsSet = selectedColumns.toSet
  private val selectedColumnsIndexed = selectedColumns.toIndexedSeq

  private def checkMissingProperties(requestedPropertyNames: Seq[String]) {
    val availablePropertyNames = PropertyExtractor.availablePropertyNames(cls, requestedPropertyNames)
    val missingColumns = requestedPropertyNames.toSet -- availablePropertyNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"One or more properties not found in RDD data: ${missingColumns.mkString(", ")}")
  }

  private def checkMissingColumns(columnNames: Seq[String]) {
    val allColumnNames = table.allColumns.map(_.columnName)
    val missingColumns = columnNames.toSet -- allColumnNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Column(s) not found: ${missingColumns.mkString(", ")}")
  }

  private def checkMissingPrimaryKeyColumns(columnNames: Seq[String]) {
    val primaryKeyColumnNames = table.primaryKey.map(_.columnName)
    val missingPrimaryKeyColumns = primaryKeyColumnNames.toSet -- columnNames
    if (missingPrimaryKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some primary key columns are missing in RDD or have not been selected: ${missingPrimaryKeyColumns.mkString(", ")} (required primary key columns are: $primaryKeyColumnNames and all columns are $columnNames, table is: $table)")
  }

  private def columnNameByRef(columnRef: ColumnRef): Option[String] = {
    columnRef match {
      case ColumnName(name) if selectedColumnsSet.contains(name) => Some(name)
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
  checkMissingColumns(columnNames)
  checkMissingPrimaryKeyColumns(columnNames)

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
    override def rowWriter(tableDef: TableDef, columnNames: Seq[String]) = {
      new DefaultRowWriter[T](tableDef, columnNames)
    }
  }

}
