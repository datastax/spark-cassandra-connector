package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ProtocolVersion, PreparedStatement}
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.{IndexedColumnRef, NamedColumnRef, ColumnRef, ColumnMapper}
import com.datastax.spark.connector.types.TypeConverter

import scala.collection.{Map, Seq}

import scala.collection.JavaConversions._

/** A `RowWriter` suitable for saving objects mappable by a [[com.datastax.spark.connector.mapper.ColumnMapper ColumnMapper]].
  * Can save case class objects, java beans and tuples. */
class DefaultRowWriter[T : ColumnMapper](table: TableDef, selectedColumns: Seq[String])
  extends RowWriter[T] {

  private val columnMapper = implicitly[ColumnMapper[T]]
  private val cls = columnMapper.classTag.runtimeClass.asInstanceOf[Class[T]]
  private val columnMap = columnMapper.columnMap(table)
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
        s"Some primary key columns are missing in RDD or have not been selected: ${missingPrimaryKeyColumns.mkString(", ")}")
  }

  private def columnNameByRef(columnRef: ColumnRef): Option[String] = {
    columnRef match {
      case NamedColumnRef(name) if selectedColumnsSet.contains(name) => Some(name)
      case IndexedColumnRef(index) if index < selectedColumns.size => Some(selectedColumnsIndexed(index))
      case _ => None
    }
  }

  private val columnNameToType: Map[String, TypeConverter[_]] =
    table.allColumns.map(c => (c.columnName, c.columnType.converterToCassandra)).toMap

  val (propertyNames, columnNames) = {
    val propertyToColumnName = columnMap.getters.mapValues(columnNameByRef).toSeq
    val selectedPropertyColumnPairs =
      for ((propertyName, Some(columnName)) <- propertyToColumnName if selectedColumnsSet.contains(columnName))
      yield (propertyName, columnName)
    selectedPropertyColumnPairs.unzip
  }

  private val columnNameToPropertyName = (columnNames zip propertyNames).toMap

  checkMissingProperties(propertyNames)
  checkMissingColumns(columnNames)
  checkMissingPrimaryKeyColumns(columnNames)

  private val propertyTypes = columnNames.map(columnNameToType)
  private val extractor = new ConvertingPropertyExtractor(cls, propertyNames zip propertyTypes)

  @transient
  private lazy val buffer = new ThreadLocal[Array[AnyRef]] {
    override def initialValue() = Array.ofDim[AnyRef](columnNames.size)
  }

  override def bind(data: T, stmt: PreparedStatement, protocolVersion: ProtocolVersion) = {
    val boundStmt = stmt.bind()
    for (variable <- stmt.getVariables) {
      val columnName = variable.getName
      val propertyName = columnNameToPropertyName(columnName)
      val value = extractor.extractProperty(data, propertyName)
      val serializedValue =
        if (value != null)
          variable.getType.serialize(value, protocolVersion)
        else
          null
      boundStmt.setBytesUnsafe(columnName, serializedValue)
    }
    boundStmt
  }

  private def fillBuffer(data: T): Array[AnyRef] =
    extractor.extract(data, buffer.get)

  override def estimateSizeInBytes(data: T) = {
    ObjectSizeEstimator.measureSerializedSize(fillBuffer(data))
  }
}

object DefaultRowWriter {

  def factory[T : ColumnMapper] = new RowWriterFactory[T] {
    override def rowWriter(tableDef: TableDef, columnNames: Seq[String]) = {
      new DefaultRowWriter[T](tableDef, columnNames)
    }
  }

}
