package com.datastax.spark.connector.bulk

import org.apache.cassandra.utils.OutputHandler
import org.slf4j.Logger

class BulkOutputHandler(log: Logger) extends OutputHandler {
  override def warn(msg: String): Unit = log.warn(msg)

  override def warn(msg: String, th: Throwable): Unit = log.warn(msg, th)

  override def debug(msg: String): Unit = log.debug(msg)

  override def output(msg: String): Unit = log.info(msg)
}
