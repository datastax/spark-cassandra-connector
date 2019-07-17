package com.datastax.spark.connector.writer

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.spark.connector.writer.AsyncExecutor.Handler

class QueryExecutor(
 session: CqlSession,
 maxConcurrentQueries: Int,
 successHandler: Option[Handler[RichStatement]],
 failureHandler: Option[Handler[RichStatement]])

  extends AsyncExecutor[RichStatement, AsyncResultSet](
    stmt => session.executeAsync(stmt.stmt),
    maxConcurrentQueries,
    successHandler,
    failureHandler)

object QueryExecutor {

  /**
    * Builds a query executor whose max requests per connection is limited to the MaxRequests per Connection
    */
  def apply(
    session: CqlSession,
    maxConcurrentQueries: Int,
    successHandler: Option[Handler[RichStatement]],
    failureHandler: Option[Handler[RichStatement]]): QueryExecutor = {

    new QueryExecutor(session, maxConcurrentQueries, successHandler, failureHandler)
  }
}
