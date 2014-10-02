package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper

import scala.reflect.runtime.universe._

class KeyValueRowReaderFactory[K: TypeTag : ColumnMapper, V: TypeTag : ColumnMapper]
  extends RowReaderFactory[(K, V)] {

  override def rowReader(table: TableDef): RowReader[(K, V)] = {
    val keyReader = new ClassBasedRowReader[K](table)
    val keyIsTuple = typeTag[K].tpe.typeSymbol.fullName startsWith "scala.Tuple"
    val skipColumns = if (keyIsTuple) keyReader.factory.argCount else 0
    val valueReader = new ClassBasedRowReader[V](table, skipColumns)
    new KeyValueRowReader(keyReader, valueReader)
  }
}

class KeyValueRowReader[K, V](keyReader: RowReader[K], valueReader: RowReader[V]) extends RowReader[(K, V)] {

  override def columnCount: Option[Int] = None
  override def columnNames: Option[Seq[String]] = None

  override def read(row: Row, columnNames: Array[String]): (K, V) = {
    (keyReader.read(row, columnNames), valueReader.read(row, columnNames))
  }

}
