package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.{Row, ResultSet}

/** Allows to efficiently iterate over a large, paged ResultSet,
  * asynchronously prefetching the next page.
  * 
  * @param resultSet result set obtained from the Java driver
  * @param prefetchWindowSize if there are less than this rows available without blocking,
  *                           initiates fetching the next page
  */
class PrefetchingResultSetIterator(resultSet: ResultSet, prefetchWindowSize: Int) extends Iterator[Row] {

  private[this] val iterator = resultSet.iterator()

  override def hasNext = iterator.hasNext

  private[this] def maybePrefetch(): Unit = {
    if (!resultSet.isFullyFetched && resultSet.getAvailableWithoutFetching < prefetchWindowSize)
      resultSet.fetchMoreResults()
  }

  override def next() = {
    maybePrefetch()
    iterator.next()
  }
}
