package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ResultSet, Statement, Session}

class QueryExecutor(session: Session, maxConcurrentQueries: Int,
    successHandler: Option[RichStatement => Unit], failureHandler: Option[RichStatement => Unit])
    extends AsyncExecutor[RichStatement, ResultSet](stmt => session.executeAsync(stmt.asInstanceOf[Statement]),
      maxConcurrentQueries, successHandler, failureHandler)

