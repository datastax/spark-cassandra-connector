package com.datastax.spark.connector.writer

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.cql.TableDef

/** A [[RowWriter]] that can write [[CassandraRow]] objects.*/
class GenericRowWriter(table: TableDef, selectedColumns: Seq[String]) extends AbstractRowWriter[CassandraRow](table: TableDef, selectedColumns: Seq[String]) {

  override protected def getColumnValue(data: CassandraRow, columnName: String): AnyRef = {
    val index = data.indexOf(columnName)
    if (index >= 0) {
      val converter = table.columnByName(columnName).columnType.converterToCassandra
      val value = data.get[AnyRef](index)
      converter.convert(value).asInstanceOf[AnyRef]
    }
    else
      null
  }
}


object GenericRowWriter {

  object Factory extends RowWriterFactory[CassandraRow] {
    override def rowWriter(table: TableDef, columnNames: Seq[String]) =
      new GenericRowWriter(table, columnNames)
  }

}
