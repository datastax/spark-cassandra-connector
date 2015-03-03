package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import com.datastax.spark.connector.types.ColumnType
import org.apache.spark.Logging

private[connector] class BoundStatementBuilder[T](
    val rowWriter: RowWriter[T],
    val preparedStmt: PreparedStatement,
    val protocolVersion: ProtocolVersion) extends Logging {

  private val columnNames = rowWriter.columnNames.toIndexedSeq
  private val columnTypes = columnNames.map(preparedStmt.getVariables.getType)
  private val converters = columnTypes.map(ColumnType.converterToCassandra(_)(protocolVersion))
  private val buffer = Array.ofDim[Any](columnNames.size)

  /** Creates `BoundStatement` from the given data item */
  def bind(row: T): RichBoundStatement = {
    val boundStatement = new RichBoundStatement(preparedStmt)
    rowWriter.readColumnValues(row, buffer)
    var bytesCount = 0
    for (i <- 0 until columnNames.size) {
      val converter = converters(i)
      val columnName = columnNames(i)
      val columnValue = converter.convert(buffer(i))
      val columnType = columnTypes(i)
      val serializedValue =
        if (columnValue != null) columnType.serialize(columnValue, protocolVersion)
        else null

      if (serializedValue != null)
        bytesCount += serializedValue.remaining()

      boundStatement.setBytesUnsafe(columnName, serializedValue)
    }
    boundStatement.bytesCount = bytesCount
    boundStatement
  }

}

private[connector] object BoundStatementBuilder {
  /** Calculate bound statement size in bytes. */
  def calculateDataSize(stmt: BoundStatement): Int = {
    var size = 0
    for (i <- 0 until stmt.preparedStatement().getVariables.size())
      if (!stmt.isNull(i)) size += stmt.getBytesUnsafe(i).remaining()

    size
  }
}
