package com.datastax.spark.connector.writer

import java.util

import com.datastax.driver.core.{BatchStatement, BoundStatement, PreparedStatement, Statement}

trait RichStatement {
  private[connector] def bytesCount: Int
  private[connector] def rowsCount: Int
}

class RichBoundStatement(stmt: PreparedStatement) extends BoundStatement(stmt) with RichStatement {
  private[connector] var bytesCount = 0
  private[connector] val rowsCount = 1
}

class RichBatchStatement(batchType: BatchStatement.Type, stmts: Seq[RichBoundStatement])
    extends BatchStatement(batchType) with RichStatement {

  // a small optimisation
  RichBatchStatement.ensureCapacity(this, stmts.size)
  private[connector] var bytesCount = 0
  for (stmt <- stmts) {
    add(stmt)
    bytesCount += stmt.bytesCount
  }

  private[connector] def rowsCount = size()

}

object RichBatchStatement {
  private val statementsField = classOf[BatchStatement].getDeclaredField("statements")
  private def ensureCapacity(batchStatement: BatchStatement, expectedCapacity: Int): Unit = {
    // TODO remove this workaround when https://datastax-oss.atlassian.net/browse/JAVA-649 is fixed
    statementsField.setAccessible(true)
    statementsField.get(batchStatement)
        .asInstanceOf[util.ArrayList[Statement]]
        .ensureCapacity(expectedCapacity)
  }
}
