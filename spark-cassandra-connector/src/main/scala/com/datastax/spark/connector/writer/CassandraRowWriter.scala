package com.datastax.spark.connector.writer

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.cql.TableDef

import scala.reflect.ClassTag

/** A [[RowWriter]] that can write [[CassandraRow]] objects.*/
class CassandraRowWriter(table: TableDef, selectedColumns: Seq[String]) extends RowWriter[CassandraRow] {

  val columnNames = selectedColumns

  override def readColumnValues(data: CassandraRow, buffer: Array[Any]) = {
    for ((c, i) <- columnNames.zipWithIndex)
      buffer(i) = data.getRaw(c)
  }
}


object CassandraRowWriter {

  object Factory extends RowWriterFactory[CassandraRow] {
    override def rowWriter(table: TableDef, columnNames: Seq[String], aliasToColumnName: Map[String, String]) =
      new CassandraRowWriter(table, columnNames)

    override def classTag: ClassTag[CassandraRow] = ClassTag[CassandraRow](CassandraRow.getClass)
  }

}
