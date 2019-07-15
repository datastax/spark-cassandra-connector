package com.datastax.spark.connector.writer

import java.util

import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType, BoundStatement, Statement}
import com.datastax.oss.driver.internal.core.cql.{DefaultBatchStatement, DefaultBoundStatement}

trait RichStatement {
  def bytesCount: Int
  def rowsCount: Int
}

private[connector] class RichBoundStatement(stmt: BoundStatement) extends DefaultBoundStatement(stmt) with RichStatement {
  var bytesCount = 0
  val rowsCount = 1
}

private[connector] class RichBatchStatement(batchType: BatchType, stmts: Seq[RichBoundStatement])
    extends DefaultBatchStatement(batchType) with RichStatement {

  // a small optimisation
  RichBatchStatement.ensureCapacity(this, stmts.size)
  var bytesCount = 0
  for (stmt <- stmts) {
    add(stmt)
    bytesCount += stmt.bytesCount
  }

  def rowsCount = size()
}

private[connector] object RichBatchStatement {
  private val statementsField = classOf[BatchStatement].getDeclaredField("statements")
  def ensureCapacity(batchStatement: BatchStatement, expectedCapacity: Int): Unit = {
    // TODO remove this workaround when https://datastax-oss.atlassian.net/browse/JAVA-649 is fixed
    statementsField.setAccessible(true)
    statementsField.get(batchStatement)
        .asInstanceOf[util.ArrayList[Statement]]
        .ensureCapacity(expectedCapacity)
  }
}
