package com.datastax.spark.connector.cql

import com.datastax.bdp.util.ScalaJavaUtil.asScalaFuture
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{Row, Statement}
import com.datastax.spark.connector.CassandraRowMetadata
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.rdd.reader.PrefetchingResultSetIterator
import com.datastax.spark.connector.util.maybeExecutingAs
import com.datastax.spark.connector.writer.RateLimiter

import scala.concurrent.duration.Duration
import scala.concurrent.{Await}

/**
  * Object which will be used in Table Scanning Operations.
  * One Scanner will be created per Spark Partition, it will be
  * created at the beginning of the compute method and Closed at the
  * end of the compute method.
  */
trait Scanner {
  def close(): Unit
  def getSession(): CqlSession
  def scan[StatementT <: Statement[StatementT]](statement: StatementT): ScanResult
}

case class ScanResult (rows: Iterator[Row], metadata: CassandraRowMetadata)

class DefaultScanner (
    readConf: ReadConf,
    connConf: CassandraConnectorConf,
    columnNames: IndexedSeq[String]) extends Scanner {

  private val session = new CassandraConnector(connConf).openSession()
  private val codecRegistry = session.getContext.getCodecRegistry

  override def close(): Unit = {
    session.close()
  }

  override def scan[StatementT <: Statement[StatementT]](statement: StatementT): ScanResult = {
    import com.datastax.spark.connector.util.Threads.BlockingIOExecutionContext

    val rs = session.executeAsync(maybeExecutingAs(statement, readConf.executeAs))
    val scanResult = asScalaFuture(rs).map { rs =>
      val columnMetaData = CassandraRowMetadata.fromResultSet(columnNames, rs, codecRegistry)
      val prefetchingIterator = new PrefetchingResultSetIterator(rs, readConf.fetchSizeInRows)
      val rateLimitingIterator = readConf.throughputMiBPS match {
        case Some(throughput) =>
          val rateLimiter = new RateLimiter((throughput * 1024 * 1024).toLong, 1024 * 1024)
          prefetchingIterator.map { row =>
            rateLimiter.maybeSleep(getRowBinarySize(row))
            row
          }
        case None =>
          prefetchingIterator
      }
      ScanResult(rateLimitingIterator, columnMetaData)
    }
    Await.result(scanResult, Duration.Inf)
  }

  override def getSession(): CqlSession = session
}
