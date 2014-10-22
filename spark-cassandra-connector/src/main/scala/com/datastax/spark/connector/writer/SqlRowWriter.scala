package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.TableDef
import org.apache.spark.sql.catalyst.expressions.Row

/** A [[RowWriter]] that can write [[Row]] objects.*/
class SqlRowWriter(table: TableDef, selectedColumns: Seq[String]) extends AbstractRowWriter[Row](table: TableDef, selectedColumns: Seq[String]) {

  override protected def getColumnValue(data: Row, columnName: String): AnyRef = {
    val index = columnNames.indexOf(columnName)
    if (index >= 0 && index < data.size) {
      val converter = table.columnByName(columnName).columnType.converterToCassandra
      val value = data.apply(index)
      if (value == null) null else converter.convert(value).asInstanceOf[AnyRef]
    }
    else
      null
  }
}


object SqlRowWriter {

  object Factory extends RowWriterFactory[Row] {
    override def rowWriter(table: TableDef, columnNames: Seq[String]) =
      new SqlRowWriter(table, columnNames)
  }

}