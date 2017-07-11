package com.datastax.spark.connector

import com.datastax.driver.core.{BatchStatement, BoundStatement}

/** Useful stuff that didn't fit elsewhere. */
package object util {

  def maybeExecutingAs[T](stmt: T, proxyUser: Option[String]): T = {
    (proxyUser, stmt) match {
      case (Some(user), bs: BoundStatement) =>
        bs.executingAs(user)
      case (Some(user), bs: BatchStatement) =>
        bs.executingAs(user)
      case _ =>
    }
    stmt
  }

}
