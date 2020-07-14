package com.datastax.spark.connector.writer

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector.{CassandraRow, ColumnRef}
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.types.ColumnType

/** A [[RowWriter]] that can write [[CassandraRow]] objects.*/
class CassandraRowWriter(table: TableMetadata, selectedColumns: IndexedSeq[ColumnRef]) extends RowWriter[CassandraRow] {

  implicit val tableMetadataArg = table

  override val columnNames = selectedColumns.map(_.columnName)

  private val columns = columnNames.map(TableDef.columnByName).toIndexedSeq
  private val converters = columns.map(col => ColumnType.fromDriverType(col.getType).converterToCassandra)

  override def readColumnValues(data: CassandraRow, buffer: Array[Any]) = {
    for ((c, i) <- columnNames.zipWithIndex) {
      val value = data.getRaw(c)
      val convertedValue = converters(i).convert(value)
      buffer(i) = convertedValue
    }
  }
}


object CassandraRowWriter {

  object Factory extends RowWriterFactory[CassandraRow] {
    override def rowWriter(table: TableMetadata, selectedColumns: IndexedSeq[ColumnRef]) =
      new CassandraRowWriter(table, selectedColumns)
  }

}
