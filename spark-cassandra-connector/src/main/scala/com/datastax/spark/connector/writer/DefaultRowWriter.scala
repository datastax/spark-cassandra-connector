package com.datastax.spark.connector.writer

import scala.reflect.ClassTag

import com.datastax.spark.connector.{ColumnName, ColumnRef}
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper

import scala.collection.Seq
import scala.collection.JavaConversions._

/** A `RowWriter` suitable for saving objects mappable by a [[com.datastax.spark.connector.mapper.ColumnMapper ColumnMapper]].
  * Can save case class objects, java beans and tuples. */
class DefaultRowWriter[T : ColumnMapper : ClassTag](
    table: TableDef, 
    selectedColumns: IndexedSeq[ColumnRef])
  extends RowWriter[T] {

  // do not save reference to ColumnMapper in a field, because it is non Serializable
  private val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
  private val columnMap = implicitly[ColumnMapper[T]].columnMapForWriting(table, selectedColumns)

  private def checkMissingProperties(requestedPropertyNames: Seq[String]) {
    val availablePropertyNames = PropertyExtractor.availablePropertyNames(cls, requestedPropertyNames)
    val missingColumns = requestedPropertyNames.toSet -- availablePropertyNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"One or more properties not found in RDD data: ${missingColumns.mkString(", ")}")
  }

  // the column map contains only the properties present in selectedColumns already
  val (propertyNames, columnNames) =
    columnMap.getters.mapValues(_.columnName).toSeq.unzip

  checkMissingProperties(propertyNames)

  private val columnNameToPropertyName = (columnNames zip propertyNames).toMap
  private val extractor = new PropertyExtractor(cls, propertyNames)
  private val columns = columnNames.map(table.columnByName).toIndexedSeq
  private val converters = columns.map(_.columnType.converterToCassandra)

  override def readColumnValues(data: T, buffer: Array[Any]) = {
    for ((c, i) <- columnNames.zipWithIndex) {
      val propertyName = columnNameToPropertyName(c)
      val value = extractor.extractProperty(data, propertyName)
      val convertedValue = converters(i).convert(value)
      buffer(i) = convertedValue
    }
  }
}

object DefaultRowWriter {

  def factory[T : ColumnMapper : ClassTag] = new RowWriterFactory[T] {
    override def rowWriter(tableDef: TableDef, selectedColumns: IndexedSeq[ColumnRef]) = {
      new DefaultRowWriter[T](tableDef, selectedColumns)
    }
  }
}

