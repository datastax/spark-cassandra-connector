package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper

import scala.reflect.runtime.universe._

class KV[K, V](k: K, v: V) extends (K, V)(k, v)

class KVBasedRowReaderFactory[K: TypeTag : ColumnMapper, V: TypeTag : ColumnMapper]
  extends RowReaderFactory[KV[K, V]] {

  override def rowReader(table: TableDef): RowReader[KV[K, V]] = {
    val keyReader = new ClassBasedRowReader[K](table)
    val keyIsTuple = typeTag[K].tpe.typeSymbol.fullName startsWith "scala.Tuple"
    val skipColumns = if (keyIsTuple) keyReader.factory.argCount else 0

    val valueReader = new ClassBasedRowReader[V](table, skipColumns)

    new KVRowReader(keyReader, valueReader)
  }
}

class KVRowReader[K, V](keyReader: RowReader[K], valueReader: RowReader[V]) extends RowReader[KV[K, V]] {

  override def columnCount: Option[Int] = None

  override def columnNames: Option[Seq[String]] = None

  override def read(row: Row, columnNames: Array[String]) = {
    new KV[K, V](keyReader.read(row, columnNames), valueReader.read(row, columnNames))
  }

}
