package com.datastax.spark.connector.mapper

import com.datastax.driver.core.ProtocolVersion
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.ColumnType
import org.apache.spark.sql.types.StructType

class DataFrameColumnMapper[T](structType: StructType) extends ColumnMapper[T] {
  override def columnMapForWriting(struct: StructDef,
                                   selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForWriting = ???

  override def columnMapForReading(struct: StructDef,
                                   selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForReading = ???

  override def newTable(
    keyspaceName: String,
    tableName: String,
    protocolVersion: ProtocolVersion = ProtocolVersion.NEWEST_SUPPORTED): TableDef = {

    val columns = structType.zipWithIndex.map { case (field, i) => {
      val columnRole = if (i == 0) PartitionKeyColumn else RegularColumn
      ColumnDef(field.name, columnRole, ColumnType.fromSparkSqlType(field.dataType, protocolVersion))
    }}

    TableDef(keyspaceName, tableName, Seq(columns.head), Seq.empty, columns.tail)
  }
}
