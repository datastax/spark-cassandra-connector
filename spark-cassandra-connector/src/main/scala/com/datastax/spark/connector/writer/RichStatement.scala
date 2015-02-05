package com.datastax.spark.connector.writer

import java.util

import com.datastax.driver.core.{BatchStatement, BoundStatement, PreparedStatement, Statement}

trait RichStatement {
  private[writer] def bytesCount: Int
  private[writer] def rowsCount: Int
}

class RichBoundStatement(stmt: PreparedStatement) extends BoundStatement(stmt) with RichStatement {
  private[writer] var bytesCount = 0
  private[writer] val rowsCount = 1
}

class RichBatchStatement(batchType: BatchStatement.Type, stmts: Seq[RichBoundStatement])
    extends BatchStatement(batchType) with RichStatement {

  // a small optimisation
  RichBatchStatement.ensureCapacity(this, stmts.size)
  private[writer] var bytesCount = 0
  for (stmt <- stmts) {
    add(stmt)
    bytesCount += stmt.bytesCount
  }

  private[writer] def rowsCount = size()

}

object RichBatchStatement {
  private val statementsField = classOf[BatchStatement].getDeclaredField("statements")
  private def ensureCapacity(batchStatement: BatchStatement, expectedCapacity: Int): Unit = {
    statementsField.setAccessible(true)
    statementsField.get(batchStatement)
        .asInstanceOf[util.ArrayList[Statement]]
        .ensureCapacity(expectedCapacity)
    statementsField.setAccessible(false)
  }
}
