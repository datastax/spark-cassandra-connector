package com.datastax.spark.connector.writer

import java.nio.ByteBuffer

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql._
import com.datastax.spark.connector.util.maybeExecutingAs
import com.datastax.spark.connector.writer.RichStatement.DriverStatement

trait RichStatement {
  def bytesCount: Int
  def rowsCount: Int
  def stmt: DriverStatement
  def executeAs(executeAs: Option[String]): RichStatement
}

object RichStatement {
  type DriverStatement = Statement[_ <: Statement[_]]
}

private[connector] class RichBoundStatementWrapper(initStatement: BoundStatement)
  extends RichStatement {

  def update(updateFunction: BoundStatement => BoundStatement): Unit = {
    _stmt = updateFunction(_stmt)
  }

  private var _stmt = initStatement
  var bytesCount = 0
  val rowsCount = 1

  def setConsistencyLevel(consistencyLevel: ConsistencyLevel): RichBoundStatementWrapper = {
    _stmt = _stmt.setConsistencyLevel(consistencyLevel)
    this
  }

  override def stmt: BoundStatement = _stmt

  override def executeAs(executeAs: Option[String]): RichStatement = {
    _stmt = maybeExecutingAs(_stmt, executeAs)
    this
  }
}

private[connector] class RichBatchStatementWrapper(
    batchType: BatchType,
    consistencyLevel: ConsistencyLevel,
    stmts: Seq[RichBoundStatementWrapper])
  extends RichStatement {

  private var _stmt = BatchStatement.newInstance(batchType, stmts.map(_.stmt):_*).setConsistencyLevel(consistencyLevel)

  override val bytesCount: Int = stmts.map(_.bytesCount).sum

  override val rowsCount = _stmt.size()

  override def stmt: BatchStatement = _stmt

  override def executeAs(executeAs: Option[String]): RichStatement = {
    _stmt = maybeExecutingAs(_stmt, executeAs)
    this
  }
}
