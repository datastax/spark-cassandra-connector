package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.{ProtocolVersion, Row}
import com.datastax.spark.connector.{ColumnSelector, ColumnRef}
import com.datastax.spark.connector.cql.TableDef

private[connector] class KeyValueRowReaderFactory[K, V](
    keySelection: ColumnSelector,
    keyRRF: RowReaderFactory[K],
    valueRRF: RowReaderFactory[V])
  extends RowReaderFactory[(K, V)] {

  override def rowReader(table: TableDef, columnSelection: IndexedSeq[ColumnRef]): RowReader[(K, V)] = {
    val keyReader = keyRRF.rowReader(table, keySelection.selectFrom(table))
    val valueReader = valueRRF.rowReader(table, columnSelection)
    new KeyValueRowReader(keyReader, valueReader)
  }

  override def targetClass: Class[(K, V)] = classOf[(K, V)]
}

private[connector] class KeyValueRowReader[K, V](keyReader: RowReader[K], valueReader: RowReader[V])
  extends RowReader[(K, V)] {

  override def neededColumns: Option[Seq[ColumnRef]] =
    (for (keyNames <- keyReader.neededColumns; valueNames <- valueReader.neededColumns) yield keyNames ++ valueNames)
      .orElse(keyReader.neededColumns).orElse(valueReader.neededColumns)

  override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion): (K, V) = {
    (keyReader.read(row, columnNames), valueReader.read(row, columnNames))
  }
}
