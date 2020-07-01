/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.spark

import java.io.IOException

import scala.collection.JavaConverters._
import com.datastax.dse.driver.api.core.cql.continuous.{ContinuousResultSet, ContinuousSession}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.cql.{BoundStatement, Statement}
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException
import com.datastax.spark.connector.CassandraRowMetadata
import com.datastax.spark.connector.cql.{CassandraConnectorConf, _}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.DriverUtil.toName
import com.datastax.spark.connector.util._

case class ContinuousPagingScanner(
  readConf: ReadConf,
  connConf: CassandraConnectorConf,
  columnNames: IndexedSeq[String]) extends Scanner with Logging {

  val TARGET_PAGE_SIZE_IN_BYTES: Int = 5000 * 50 // 5000 rows * 50 bytes per row
  val MIN_PAGES_PER_SECOND = 1000

  //TODO This must be moved to session initilization? We can no longer pass options at execution time without deriving a new profile
  // I think the right thing to do, to support old configurations as well as new is to create a new profile based on options as
  // Set using derivied profiles, but this probably can't happen here
  /**
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
    **/

  /**
    * Attempts to get or create a session for this execution thread.
    */
  private val cpSession = new CassandraConnector(connConf)
    .openSession()
    .asInstanceOf[CqlSession with ContinuousSession]

  private val codecRegistry = cpSession.getContext.getCodecRegistry

  private def asBoundStatement(statement: Statement[_]): Option[BoundStatement] = {
    statement match {
      case s: BoundStatement => Some(s)
      case _ => None
    }
  }

  def isSolr(statement: Statement[_]): Boolean = {
    asBoundStatement(statement).exists(_.getPreparedStatement.getQuery.contains("solr_query"))
  }

  /**
    * Calls SessionProxy Close which issues a deferred close request on the session if no
    * references are requested to it in the next keep_alive ms
    */
  override def close(): Unit = cpSession.close

  override def getSession(): CqlSession = cpSession

  override def scan[StatementT <: Statement[StatementT]](statement: StatementT): ScanResult = {
     val authStatement = maybeExecutingAs(statement, readConf.executeAs)

    if (isSolr(authStatement)) {
      logDebug("Continuous Paging doesn't work with Search, Falling back to default paging")
      val regularResult = cpSession.execute(authStatement)
      val regularIterator = regularResult.iterator().asScala
      ScanResult(regularIterator, CassandraRowMetadata.fromResultSet(columnNames, regularResult, codecRegistry))

    } else {
      try {
        val cpResult = cpSession.executeContinuously(authStatement)
        val cpIterator = cpResult.iterator().asScala
        ScanResult(cpIterator, getMetaData(cpResult))
      } catch {
        case e: InvalidQueryException if e.getMessage.contains("too many continuous paging sessions are already running") =>
          throw new IOException(s"${e.getMessage}. This error may be intermittent, if there are other applications " +
            s"using continuous paging wait for them to finish and re-execute. If the error persists adjust your DSE " +
            s"server setting `continuous_paging.max_concurrent_sessions` or lower the parallelism level of this job " +
            s"(reduce the number of executors and/or assigned cores) or disable continuous paging for this app " +
            s"with ${CassandraConnectionFactory.continuousPagingParam.name}.", e)
      }
    }
  }

  private def getMetaData(result: ContinuousResultSet) = {
    import scala.collection.JavaConverters._
    val columnDefs = result.getColumnDefinitions.asScala
    val rsColumnNames = columnDefs.map(c => toName(c.getName))
    val codecs = columnDefs
      .map(col => codecRegistry.codecFor(col.getType))
      .asInstanceOf[Seq[TypeCodec[AnyRef]]]

    CassandraRowMetadata(columnNames, Some(rsColumnNames.toIndexedSeq), codecs.toIndexedSeq)
  }
}
