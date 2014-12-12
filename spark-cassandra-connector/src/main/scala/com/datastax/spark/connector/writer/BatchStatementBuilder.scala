package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import com.datastax.spark.connector.util.Logging
import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch}

import scala.collection.Iterator
import scala.collection.JavaConversions._

/** This class is responsible for converting a stream of data items into stream of statements which are
  * ready to be executed with Java Driver. Depending on provided options the statements are grouped into
  * batches or not. */
class BatchStatementBuilder[T](batchType: BatchStatement.Type, rowWriter: RowWriter[T], stmt: PreparedStatement,
                    protocolVersion: ProtocolVersion) extends Logging {

  import com.datastax.spark.connector.writer.BatchStatementBuilder._

  /** Converts a sequence of statements into a batch if its size is greater than 1. */
  private def maybeCreateBatch(stmts: Seq[BoundStatement]): Statement = {
    require(stmts.size > 0, "Statements list cannot be empty")

    if (stmts.size == 1) {
      stmts.head
    } else {
      new BatchStatement(batchType).addAll(stmts)
    }
  }

  /** Splits data items into groups of equal number of elements and make batches from these groups. */
  private def rowsLimitedBatches(data: Iterator[T], batchSizeInRows: Int): Stream[Statement] = {
    val batches = for (batch <- data.grouped(batchSizeInRows)) yield {
      val boundStmts = batch.map(row => rowWriter.bind(row, stmt, protocolVersion))
      maybeCreateBatch(boundStmts)
    }

    batches.toStream
  }

  /** Splits data items into groups of size not greater than the provided limit in bytes and make batches
    * from these groups. */
  private def sizeLimitedBatches(data: Iterator[T], batchSizeInBytes: Int): Stream[Statement] = {
    val boundStmts = data.toStream.map(row => rowWriter.bind(row, stmt, protocolVersion))

    def batchesStream(stmtsStream: Stream[BoundStatement]): Stream[Statement] = stmtsStream match {
      case Stream.Empty => Stream.Empty
      case head #:: rest =>
        val stmtSizes = rest.map(calculateDataSize)
        val cumulativeStmtSizes = stmtSizes.scanLeft(calculateDataSize(head).toLong)(_ + _).tail
        val addStmts = (rest zip cumulativeStmtSizes)
          .takeWhile { case (_, batchSize) => batchSize <= batchSizeInBytes}
          .map { case (boundStmt, _) => boundStmt}
        val stmtsGroup = (head #:: addStmts).toVector
        val batch = maybeCreateBatch(stmtsGroup)
        val remaining = stmtsStream.drop(stmtsGroup.size)
        batch #:: batchesStream(remaining)
    }

    batchesStream(boundStmts)
  }

  /** Just make bound statements from data items. */
  private def boundStatements(data: Iterator[T]): Stream[Statement] =
    data.map(rowWriter.bind(_, stmt, protocolVersion)).toStream

  /** Depending on provided batch size, convert the data items into stream of bound statements, batches
    * of equal rows number or batches of equal size in bytes. */
  private[connector] def makeStatements(data: Iterator[T], batchSize: BatchSize): Stream[Statement] = {
    batchSize match {
      case RowsInBatch(1) | BytesInBatch(0) =>
        logInfo(s"Creating bound statements - batch size is $batchSize")
        boundStatements(data)
      case RowsInBatch(n) =>
        logInfo(s"Creating batches of $n rows")
        rowsLimitedBatches(data, n)
      case BytesInBatch(n) =>
        logInfo(s"Creating batches of $n bytes")
        sizeLimitedBatches(data, n)
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
