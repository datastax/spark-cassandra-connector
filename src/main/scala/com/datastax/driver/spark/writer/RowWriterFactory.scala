package com.datastax.driver.spark.writer

import com.datastax.driver.spark.connector.TableDef
import com.datastax.driver.spark.mapper.ColumnMapper

import scala.reflect.ClassTag

/** Creates instances of [[RowWriter]] objects for the given row type `T`.
  * `RowWriterFactory` is the trait you need to implement if you want to support row representations
  * which cannot be simply mapped by a [[com.datastax.driver.spark.mapper.ColumnMapper ColumnMapper]].*/
trait RowWriterFactory[T] {

  /** Creates a new `RowWriter` instance.
    * @param table target table the user wants to write into
    * @param columnNames columns selected by the user; the user might wish to write only a subset of columns */
  def rowWriter(table: TableDef, columnNames: Seq[String]): RowWriter[T]
}

object RowWriterFactory {
  implicit def defaultRowWriterFactory[T : ClassTag : ColumnMapper]: RowWriterFactory[T] = DefaultRowWriter.factory
}