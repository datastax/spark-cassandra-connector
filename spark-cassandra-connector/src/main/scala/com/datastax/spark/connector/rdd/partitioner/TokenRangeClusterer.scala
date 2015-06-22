package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import com.datastax.spark.connector.rdd.partitioner.dht.{CassandraNode, Token, TokenRange}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/** Divides a set of token ranges into groups containing not more than `maxRowCountPerGroup` rows
  * and not more than `maxGroupSize` token ranges. Each group will form a single `CassandraRDDPartition`.
  *
  * The algorithm is as follows:
  * 1. Sort token ranges by endpoints lexicographically.
  * 2. Take the highest possible number of token ranges from the beginning of the list,
  *    such that their sum of rowCounts does not exceed `maxRowCountPerGroup` and they all contain at
  *    least one common endpoint. If it is not possible, take at least one item.
  *    Those token ranges will make a group.
  * 3. Repeat the previous step until no more token ranges left.*/
class TokenRangeClusterer[V, T <: Token[V]](maxRowCountPerGroup: Long, maxGroupSize: Int = Int.MaxValue) {

  private implicit object InetAddressOrdering extends Ordering[InetAddress] {
    override def compare(x: InetAddress, y: InetAddress) =
      x.getHostAddress.compareTo(y.getHostAddress)
  }



  class MutableCassandraPartition  (val index: Int,
                                            var endpoints:Set[CassandraNode] = Set(),
                                            val ranges: ArrayBuffer[TokenRange[V, T]] = ArrayBuffer(),
                                            var rowCount: Long = 0) {
    def addRange(range: TokenRange[V, T]): Unit = {
      ranges += range
      rowCount += range.rowCount.getOrElse(0l);
    }
    def addEndPoint(te:Set[CassandraNode]): Unit = {
      if (endpoints.isEmpty)
       endpoints = te
      else
       endpoints =  endpoints & te;
      assert(endpoints.nonEmpty)
    }
  }


  /** Groups small token ranges on the same server(s) in order to reduce task scheduling overhead.
    * Useful mostly with virtual nodes, which may create lots of small token range splits.
    * Each group will make a single Spark task. */
  def group(tokenRanges: Seq[TokenRange[V, T]]): Iterable[MutableCassandraPartition] = {

    val rows = tokenRanges.map(_.rowCount).flatten.sum
    val partitionNums = Math.ceil(
      Math.max((rows.toDouble/maxRowCountPerGroup),
        tokenRanges.size.toDouble/maxGroupSize)).toInt

    //TODO select appropriate type
    val partitions = (0 until partitionNums).map (new MutableCassandraPartition(_))
    val notFittedBucket = new MutableCassandraPartition(partitionNums)

    tokenRanges.foreach {
      tr =>
        val bucket = findPartition (tr, partitions.toStream, Int.MaxValue, None) match {
          // No good bucket was found, we will resort them latter.
          case None => notFittedBucket
                  //partitions(util.Random.nextInt(partitions.size))
          case Some (b) =>  b.addEndPoint(tr.endpoints); b
        }
        bucket.addRange(tr)
    }

    // if we have more then one partition with not fitted ranges
    // let's repartition int again.
    // if it is only one lft or no progress at current run, left it as is.
    val refittedPartitions = if((notFittedBucket.ranges.size > maxRowCountPerGroup
      || notFittedBucket.rowCount > maxRowCountPerGroup)
    && partitions.filter(_.ranges.nonEmpty).nonEmpty)
      group(notFittedBucket.ranges)
      else
       Seq(notFittedBucket)

      (partitions ++ refittedPartitions).filter(_.ranges.nonEmpty)
  }

  /**
   * Find best parition to put
   * @param tr
   * @param ps
   * @param bestScore
   * @param best
   * @return
   */
  @tailrec
  private def findPartition (tr: TokenRange[V,T], ps: Stream[MutableCassandraPartition],
                             bestScore: Int, best: Option[MutableCassandraPartition],
                             minimumEndPointCount: Int = 1): Option[MutableCassandraPartition] = {
    ps match {
      case Stream.Empty => best
      case head #:: rest =>
        if (head.endpoints.isEmpty)
          Some(head)
        else if ((head.rowCount + tr.rowCount.getOrElse(0l)) > maxRowCountPerGroup || head.ranges.size >= maxGroupSize)
          findPartition(tr, rest, bestScore, best, minimumEndPointCount)
        else {
          val newEndpointsCandidate = head.endpoints.intersect(tr.endpoints)
          // score is simple for now
          val score = head.endpoints.size - newEndpointsCandidate.size;
          // We will not find better bucket
          if (score == 0)
            Some(head)
          else
          //continue find minimum
          if (score < bestScore && newEndpointsCandidate.size > minimumEndPointCount)
            findPartition(tr, rest, score, Some(head), minimumEndPointCount)
          else
            findPartition(tr, rest, bestScore, best, minimumEndPointCount)
        }
    }
  }
}
