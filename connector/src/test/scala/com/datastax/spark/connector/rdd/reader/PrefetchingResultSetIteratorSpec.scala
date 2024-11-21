package com.datastax.spark.connector.rdd.reader

import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row}
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

class PrefetchingResultSetIteratorSpec extends FlatSpec with Matchers with MockitoSugar {

  "PrefetchingResultSetIterator" should "handle empty pages that are not the last with AsyncResultSet" in {
    val row1 = mock[Row]
    val row2 = mock[Row]
    val row3 = mock[Row]

    val asyncResultSet1 = mock[AsyncResultSet]
    val asyncResultSet2 = mock[AsyncResultSet]
    val asyncResultSet3 = mock[AsyncResultSet]

    // First page has data
    when(asyncResultSet1.currentPage()).thenReturn(Seq(row1).asJava)
    when(asyncResultSet1.hasMorePages()).thenReturn(true)
    when(asyncResultSet1.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(asyncResultSet2))

    // Second page is empty
    when(asyncResultSet2.currentPage()).thenReturn(Seq.empty[Row].asJava)
    when(asyncResultSet2.hasMorePages()).thenReturn(true)
    when(asyncResultSet2.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(asyncResultSet3))

    // Third page has data
    when(asyncResultSet3.currentPage()).thenReturn(Seq(row2, row3).asJava)
    when(asyncResultSet3.hasMorePages()).thenReturn(false)

    val iterator = new PrefetchingResultSetIterator(asyncResultSet1)

    val rows = iterator.toList

    rows should contain theSameElementsInOrderAs Seq(row1, row2, row3)

    verify(asyncResultSet1).fetchNextPage()
    verify(asyncResultSet2).fetchNextPage()
    verify(asyncResultSet3, never()).fetchNextPage()
  }
}