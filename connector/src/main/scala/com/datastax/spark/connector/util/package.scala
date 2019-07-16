package com.datastax.spark.connector

import com.datastax.dse.driver.api.core.auth.ProxyAuthentication
import com.datastax.oss.driver.api.core.cql.Statement

/** Useful stuff that didn't fit elsewhere. */
package object util {

  def maybeExecutingAs[StatementT <: Statement[StatementT]](stmt: StatementT, proxyUser: Option[String]): StatementT = {
    proxyUser match {
      case Some(user) =>
        ProxyAuthentication.executeAs(user, stmt)
      case _ =>
        stmt
    }
  }

}
