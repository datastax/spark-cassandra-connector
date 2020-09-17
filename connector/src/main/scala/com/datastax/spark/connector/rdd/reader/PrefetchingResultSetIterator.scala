package com.datastax.spark.connector.rdd.reader

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Timer
import com.datastax.bdp.util.ScalaJavaUtil
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row}
import com.datastax.spark.connector.util.Threads.BlockingIOExecutionContext

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/** Allows to efficiently iterate over a large, paged ResultSet,
  * asynchronously prefetching the next page.
  *
  * This iterator is NOT thread safe. Attempting to retrieve elements from many threads without synchronization
  * may yield unspecified results.
  *
  * @param resultSet result set obtained from the Java driver
  * @param timer     a Codahale timer to optionally gather the metrics of fetching time
  */
class PrefetchingResultSetIterator(resultSet: AsyncResultSet, timer: Option[Timer] = None) extends Iterator[Row] {
  private var currentIterator = resultSet.currentPage().iterator()
  private var currentResultSet = resultSet
  private var nextResultSet = fetchNextPage()

  private def fetchNextPage(): Option[Future[AsyncResultSet]] = {
    if (currentResultSet.hasMorePages) {
      val t0 = System.nanoTime();
      val next = ScalaJavaUtil.asScalaFuture(currentResultSet.fetchNextPage())
      timer.foreach { t =>
        next.foreach(_ => t.update(System.nanoTime() - t0, TimeUnit.NANOSECONDS))
      }
      Option(next)
    } else
      None
  }

  private def maybePrefetch(): Unit = {
    if (!currentIterator.hasNext && currentResultSet.hasMorePages) {
      currentResultSet = Await.result(nextResultSet.get, Duration.Inf)
      currentIterator = currentResultSet.currentPage().iterator()
      nextResultSet = fetchNextPage()
    }
  }

  override def hasNext: Boolean =
    currentIterator.hasNext || currentResultSet.hasMorePages

  override def next(): Row = {
    val row = currentIterator.next() // let's try to exhaust the current iterator first
    maybePrefetch()
    row
  }
}
