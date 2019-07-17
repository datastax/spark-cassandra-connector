package com.datastax.spark.connector.writer

import java.nio.ByteBuffer
import java.util

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType, BoundStatement, Statement}
import com.datastax.spark.connector.util.maybeExecutingAs
import com.datastax.spark.connector.writer.RichBatchStatementWrapper.DriverStatement


trait RichStatement {
  def bytesCount: Int
  def rowsCount: Int
  def stmt: DriverStatement
  def executeAs(executeAs: Option[String]): RichStatement
}

private[connector] class RichBoundStatementWrapper(initStatement: BoundStatement)
  extends RichStatement {

  private var _stmt = initStatement
  var bytesCount = 0
  val rowsCount = 1

  def setRoutingKey(buffer: ByteBuffer): RichBoundStatementWrapper = {
    _stmt = stmt.setRoutingKey(buffer)
    this
  }

  def setConsistencyLevel(consistencyLevel: ConsistencyLevel): RichBoundStatementWrapper = {
    _stmt = _stmt.setConsistencyLevel(consistencyLevel)
    this
  }

  override def stmt = _stmt

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

  private var _stmt = BatchStatement.newInstance(batchType).setConsistencyLevel(consistencyLevel)

  // a small optimisation
  RichBatchStatementWrapper.ensureCapacity(this, stmts.size)
  var bytesCount = 0
  for (s <- stmts) {
    _stmt = _stmt.add(s.stmt)
    bytesCount += s.bytesCount
  }

  override def rowsCount = _stmt.size()

  override def stmt = _stmt

  override def executeAs(executeAs: Option[String]): RichStatement = {
    _stmt = maybeExecutingAs(_stmt, executeAs)
    this
  }
}

private[connector] object RichBatchStatementWrapper {

  type DriverStatement = Statement[_ <: Statement[_]]

  private val statementsField = classOf[BatchStatement].getDeclaredField("statements")
  def ensureCapacity(batchStatement: RichBatchStatementWrapper, expectedCapacity: Int): Unit = {
    // TODO remove this workaround when https://datastax-oss.atlassian.net/browse/JAVA-649 is fixed
    statementsField.setAccessible(true)
    statementsField.get(batchStatement.stmt)
        .asInstanceOf[util.ArrayList[Statement[_]]]
        .ensureCapacity(expectedCapacity)
  }
}
