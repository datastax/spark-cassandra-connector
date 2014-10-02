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

  override def columnCount: Option[Int] =
    (for (keyCnt <- keyReader.columnCount; valueCnt <- valueReader.columnCount) yield keyCnt max valueCnt)
      .orElse(keyReader.columnCount).orElse(valueReader.columnCount)

  override def columnNames: Option[Seq[String]] =
    (for (keyNames <- keyReader.columnNames; valueNames <- valueReader.columnNames) yield keyNames ++ valueNames)
      .orElse(keyReader.columnNames).orElse(valueReader.columnNames)

  override def read(row: Row, columnNames: Array[String]): (K, V) = {
    (keyReader.read(row, columnNames), valueReader.read(row, columnNames))
  }

}
