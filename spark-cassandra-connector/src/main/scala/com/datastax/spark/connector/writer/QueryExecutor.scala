package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ResultSet, Statement, Session}

import AsyncExecutor.Handler

class QueryExecutor(session: Session, maxConcurrentQueries: Int,
    successHandler: Option[Handler[RichStatement]], failureHandler: Option[Handler[RichStatement]])

    extends AsyncExecutor[RichStatement, ResultSet](
      stmt => session.executeAsync(stmt.asInstanceOf[Statement]), maxConcurrentQueries, successHandler, failureHandler)
