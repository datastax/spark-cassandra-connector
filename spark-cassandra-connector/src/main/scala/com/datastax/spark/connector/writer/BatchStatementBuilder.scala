package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.Logging

import scala.collection.JavaConversions._

class BatchStatementBuilder[T](val batchType: BatchStatement.Type,
                               val rowWriter: RowWriter[T],
                               val preparedStmt: PreparedStatement,
                               val protocolVersion: ProtocolVersion,
                               val routingKeyGenerator: RoutingKeyGenerator,
                               val consistencyLevel: ConsistencyLevel) extends Logging {


  private val columnNames = rowWriter.columnNames.toIndexedSeq
  private val columnTypes = columnNames.map(preparedStmt.getVariables.getType)
  private val converters = columnTypes.map(ColumnType.converterToCassandra(_)(protocolVersion))
  private val buffer = Array.ofDim[Any](columnNames.size)

  def bind(row: T): BoundStatement = {
    val boundStatement = preparedStmt.bind()
    rowWriter.readColumnValues(row, buffer)
    for (i <- 0 until columnNames.size) {
      val converter = converters(i)
      val columnName = columnNames(i)
      val columnValue = converter.convert(buffer(i))
      val columnType = columnTypes(i)
      val serializedValue =
        if (columnValue != null) columnType.serialize(columnValue, protocolVersion)
        else null
      boundStatement.setBytesUnsafe(columnName, serializedValue)
    }
    boundStatement
  }

  /** Converts a sequence of statements into a batch if its size is greater than 1. */
  def maybeCreateBatch(stmts: Seq[BoundStatement]): Statement = {
    require(stmts.size > 0, "Statements list cannot be empty")
    val stmt = stmts.head
    // for batch statements, it is enough to set routing key for the first statement
    stmt.setRoutingKey(routingKeyGenerator.apply(stmt))

    if (stmts.size == 1) {
      stmt.setConsistencyLevel(consistencyLevel)
      stmt
    } else {
      val batch = new BatchStatement(batchType).addAll(stmts)
      batch.setConsistencyLevel(consistencyLevel)
      batch
    }
  }

}

object BatchStatementBuilder {
  /** Calculate bound statement size in bytes. */
  def calculateDataSize(stmt: BoundStatement): Int = {
    var size = 0
    for (i <- 0 until stmt.preparedStatement().getVariables.size())
      if (!stmt.isNull(i)) size += stmt.getBytesUnsafe(i).remaining()

    size
  }
}
