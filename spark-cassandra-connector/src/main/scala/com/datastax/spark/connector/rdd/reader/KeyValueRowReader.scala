package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.{ProtocolVersion, Row}
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.TupleColumnMapper

class KeyValueRowReaderFactory[K, V](keyRRF: RowReaderFactory[K], valueRRF: RowReaderFactory[V])
  extends RowReaderFactory[(K, V)] {

  override def rowReader(table: TableDef, columnSelection: IndexedSeq[ColumnRef]): RowReader[(K, V)] = {
    val keyReader = keyRRF.rowReader(table, columnSelection)
    // TODO: A hack for now - we should replace skippedColumns by something more flexible and less magic
    val skippedColumns = valueRRF match {
      case f: ClassBasedRowReaderFactory[_] if f.columnMapper.isInstanceOf[TupleColumnMapper[_]] =>
        keyReader.consumedColumns.getOrElse(0)
      case f: ValueRowReaderFactory[_] =>
        keyReader.consumedColumns.getOrElse(0)
      case _ => 0
    }
    val valueReader = valueRRF.rowReader(table, columnSelection.drop(skippedColumns))
    new KeyValueRowReader(keyReader, valueReader)
  }

  override def targetClass: Class[(K, V)] = classOf[(K, V)]
}

class KeyValueRowReader[K, V](keyReader: RowReader[K], valueReader: RowReader[V]) extends RowReader[(K, V)] {

  override def neededColumns: Option[Seq[ColumnRef]] =
    (for (keyNames <- keyReader.neededColumns; valueNames <- valueReader.neededColumns) yield keyNames ++ valueNames)
      .orElse(keyReader.neededColumns).orElse(valueReader.neededColumns)

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion): (K, V) = {
    (keyReader.read(row, columnNames), valueReader.read(row, columnNames))
  }

  override def consumedColumns: Option[Int] =
    for (keySkip <- keyReader.consumedColumns; valueSkip <- valueReader.consumedColumns)
    yield keySkip + valueSkip
}
