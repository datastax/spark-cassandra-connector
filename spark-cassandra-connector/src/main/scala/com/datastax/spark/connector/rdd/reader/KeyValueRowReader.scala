package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.{ProtocolVersion, Row}
import com.datastax.spark.connector.cql.TableDef

class KeyValueRowReaderFactory[K, V](keyRRF: RowReaderFactory[K], valueRRF: RowReaderFactory[V])
  extends RowReaderFactory[(K, V)] {

  override def rowReader(table: TableDef, options: RowReaderOptions): RowReader[(K, V)] = {
    val keyReader = keyRRF.rowReader(table, options)
    val valueReaderOptions = options.copy(offset = options.offset + keyReader.consumedColumns.getOrElse(0))
    val valueReader = valueRRF.rowReader(table, valueReaderOptions)
    new KeyValueRowReader(keyReader, valueReader)
  }

  override def targetClass: Class[(K, V)] = classOf[(K, V)]
}

class KeyValueRowReader[K, V](keyReader: RowReader[K], valueReader: RowReader[V]) extends RowReader[(K, V)] {

  override def requiredColumns: Option[Int] =
    (for (keyCnt <- keyReader.requiredColumns; valueCnt <- valueReader.requiredColumns) yield keyCnt max valueCnt)
      .orElse(keyReader.requiredColumns).orElse(valueReader.requiredColumns)

  override def columnNames: Option[Seq[String]] =
    (for (keyNames <- keyReader.columnNames; valueNames <- valueReader.columnNames) yield keyNames ++ valueNames)
      .orElse(keyReader.columnNames).orElse(valueReader.columnNames)

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion): (K, V) = {
    (keyReader.read(row, columnNames), valueReader.read(row, columnNames))
  }

  override def consumedColumns: Option[Int] =
    for (keySkip <- keyReader.consumedColumns; valueSkip <- valueReader.consumedColumns)
    yield keySkip + valueSkip
}
