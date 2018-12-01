package com.datastax.spark.connector.writer

import com.datastax.driver.core.{HostDistance, ResultSet, Session, Statement}
import AsyncExecutor.Handler
import org.apache.spark.SparkEnv

class QueryExecutor(
 session: Session,
 maxConcurrentQueries: Int,
 successHandler: Option[Handler[RichStatement]],
 failureHandler: Option[Handler[RichStatement]])

  extends AsyncExecutor[RichStatement, ResultSet](
    stmt => session.executeAsync(stmt.asInstanceOf[Statement]),
    maxConcurrentQueries,
    successHandler,
    failureHandler)

object QueryExecutor {

  /**
    * Builds a query executor whose max requests per connection is limited to the MaxRequests per Connection
    */
  def apply(
    session: Session,
    parallelismLevel: Int,
    successHandler: Option[Handler[RichStatement]],
    failureHandler: Option[Handler[RichStatement]]): QueryExecutor = {

    val poolingOptions = session.getCluster.getConfiguration.getPoolingOptions
    val maxConcurrentQueries = (parallelismLevel)
    new QueryExecutor(session, maxConcurrentQueries, successHandler, failureHandler)
  }
}
