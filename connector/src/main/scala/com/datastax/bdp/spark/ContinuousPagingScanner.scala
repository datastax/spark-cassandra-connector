/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.spark

import scala.collection.JavaConverters._

import com.datastax.driver.core._
import com.datastax.spark.connector.CassandraRowMetadata
import com.datastax.spark.connector.cql.{CassandraConnectorConf, _}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util._

case class ContinuousPagingScanner(
  readConf: ReadConf,
  connConf: CassandraConnectorConf,
  columnNames: IndexedSeq[String]) extends Scanner with Logging {

  val TARGET_PAGE_SIZE_IN_BYTES: Int = 5000 * 50 // 5000 rows * 50 bytes per row
  val MIN_PAGES_PER_SECOND = 1000

  private val cpOptions =  readConf.throughputMiBPS match {
    case Some(throughput) =>
      val bytesPerSecond = (throughput * 1024 * 1024).toInt
      val fallBackPagesPerSecond = math.max(MIN_PAGES_PER_SECOND, bytesPerSecond / TARGET_PAGE_SIZE_IN_BYTES)
      val pagesPerSecond: Int = readConf.readsPerSec.getOrElse(fallBackPagesPerSecond)
      if (readConf.readsPerSec.isEmpty) {
        logInfo(s"Using a pages per second of $pagesPerSecond since " +
          s"${ReadConf.ReadsPerSecParam.name} is not set")
      }
      val bytesPerPage = (bytesPerSecond / pagesPerSecond ).toInt

      if (bytesPerPage <= 0) {
        throw new IllegalArgumentException(
          s"""Read Throttling set to $throughput MBPS, but with the current
             | ${ReadConf.ReadsPerSecParam.name} value of $pagesPerSecond that equates to
             | $bytesPerPage bytes per page. This number must be positive and non-zero.
           """.stripMargin)
      }

      logDebug(s"Read Throttling set to $throughput mbps. Pages of $bytesPerPage with ${readConf.readsPerSec} max" +
        s"pages per second. ${ReadConf.FetchSizeInRowsParam.name} will be ignored.")
      ContinuousPagingOptions
        .builder()
        .withPageSize(bytesPerPage, ContinuousPagingOptions.PageUnit.BYTES)
        .withMaxPagesPerSecond(pagesPerSecond)
        .build()

    case None =>
      ContinuousPagingOptions
        .builder()
        .withPageSize(readConf.fetchSizeInRows, ContinuousPagingOptions.PageUnit.ROWS)
        .withMaxPagesPerSecond(readConf.readsPerSec.getOrElse(Integer.MAX_VALUE))
        .build()
  }

  /**
    * Attempts to get or create a session for this execution thread.
    */
  private val cpSession = new CassandraConnector(connConf)
    .openSession()
    .asInstanceOf[ContinuousPagingSession]

  private val codecRegistry = cpSession.getCluster.getConfiguration.getCodecRegistry

  private def asBoundStatement(statement: Statement): Option[BoundStatement] = {
    statement match {
      case s: BoundStatement => Some(s)
      case _ => None
    }
  }

  def isSolr(statement: Statement): Boolean = {
    asBoundStatement(statement).exists(_.preparedStatement().getQueryString.contains("solr_query"))
  }

  /**
    * Calls SessionProxy Close which issues a deferred close request on the session if no
    * references are requested to it in the next keep_alive ms
    */
  override def close(): Unit = cpSession.close

  override def getSession(): Session = cpSession

  override def scan(statement: Statement): ScanResult = {
     maybeExecutingAs(statement, readConf.executeAs)

    if (isSolr(statement)) {
      logDebug("Continuous Paging doesn't work with Search, Falling back to default paging")
      val regularResult = cpSession.execute(statement)
      val regularIterator = regularResult.iterator().asScala
      ScanResult(regularIterator, CassandraRowMetadata.fromResultSet(columnNames, regularResult, codecRegistry))

    } else {
      val cpResult = cpSession.executeContinuously(statement, cpOptions)
      val cpIterator = cpResult.iterator().asScala
      ScanResult(cpIterator, getMetaData(cpResult))
    }
  }

  private def getMetaData(result: ContinuousPagingResult) = {
    import scala.collection.JavaConverters._
    val columnDefs = result.getColumnDefinitions.asList().asScala
    val rsColumnNames = columnDefs.map(_.getName)
    val codecs = columnDefs
      .map(col => codecRegistry.codecFor(col.getType))
      .asInstanceOf[Seq[TypeCodec[AnyRef]]]

    CassandraRowMetadata(columnNames, Some(rsColumnNames.toIndexedSeq), codecs.toIndexedSeq)
  }
}
