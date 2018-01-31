package com.datastax.spark.connector

import com.datastax.driver.core.{BatchStatement, BoundStatement}
import com.datastax.spark.connector.cql.ReplicaAwareStatement

/** Useful stuff that didn't fit elsewhere. */
package object util {

  def maybeExecutingAs[T](stmt: T, proxyUser: Option[String]): T = {
    (proxyUser, stmt) match {
      case (Some(user), bs: BoundStatement) =>
        bs.executingAs(user)
      case (Some(user), bs: BatchStatement) =>
        bs.executingAs(user)
      case (Some(_), ras: ReplicaAwareStatement) =>
        maybeExecutingAs(ras.wrapped, proxyUser)
      case _ =>
    }
    stmt
  }

}
