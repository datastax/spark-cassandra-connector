package com.datastax.spark.connector.writer

import com.datastax.driver.core.{Statement, Session}

class QueryExecutor(session: Session, maxConcurrentQueries: Int)
  extends AsyncExecutor(session.executeAsync(_ : Statement), maxConcurrentQueries)

