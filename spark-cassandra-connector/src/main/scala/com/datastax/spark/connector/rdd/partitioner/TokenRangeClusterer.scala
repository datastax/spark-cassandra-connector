package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import Ordering.Implicits._

import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenRange}
import scala.annotation.tailrec

import com.datastax.spark.connector.rdd.partitioner.TokenRangeClusterer.WholeRing

/** Groups a set of token ranges into `groupCount` groups containing not more than `maxGroupSize` token
  * ranges.
  * Each group will form a single `CassandraRDDPartition`.
  *
  * The algorithm is as follows:
  * 1. Sort token ranges by endpoints lexicographically.
  * 2. Take the highest possible number of token ranges from the beginning of the list,
  *    such that their sum of ringFraction does not exceed `ringFractionPerGroup` and they all contain at
  *    least one common endpoint. If it is not possible, take at least one item.
  *    Those token ranges will make a group.
  * 3. Repeat the previous step until no more token ranges left.*/
class TokenRangeClusterer[V, T <: Token[V]](groupCount: Int, maxGroupSize: Int = Int.MaxValue) {

  private val ringFractionPerGroup = WholeRing / groupCount.toDouble

  private implicit object InetAddressOrdering extends Ordering[InetAddress] {
    override def compare(x: InetAddress, y: InetAddress) =
      x.getHostAddress.compareTo(y.getHostAddress)
  }

  @tailrec
  private def group(tokenRanges: Stream[TokenRange[V, T]],
                    result: Vector[Seq[TokenRange[V, T]]],
                    ringFractionPerGroup: Double): Iterable[Seq[TokenRange[V, T]]] = {
    tokenRanges match {
      case Stream.Empty => result
      case head #:: rest =>
        val firstEndpoint = head.replicas.min
        val ringFractions = tokenRanges.map(_.ringFraction)
        val cumulativeRingFractions = ringFractions.scanLeft(.0)(_ + _).tail // drop first item always == 0
        val ringFractionLimit = math.max(ringFractionPerGroup, head.ringFraction) // make sure first element will be always included
        val cluster = tokenRanges
          .take(math.max(1, maxGroupSize))
          .zip(cumulativeRingFractions)
          .takeWhile { case (tr, count) => count <= ringFractionLimit && tr.replicas.min == firstEndpoint }
          .map(_._1)
          .toVector
        val remainingTokenRanges = tokenRanges.drop(cluster.length)
        group(remainingTokenRanges, result :+ cluster, ringFractionPerGroup)
    }
  }

  /** Groups small token ranges on the same server(s) in order to reduce task scheduling overhead.
    * Useful mostly with virtual nodes, which may create lots of small token range splits.
    * Each group will make a single Spark task. */
  def group(tokenRanges: Seq[TokenRange[V, T]]): Iterable[Seq[TokenRange[V, T]]] = {
    // sort by endpoints lexicographically
    // this way ranges on the same host are grouped together
    val sortedRanges = tokenRanges.sortBy(_.replicas.toSeq.sorted)
    group(sortedRanges.toStream, Vector.empty, ringFractionPerGroup)
  }
}

object TokenRangeClusterer {
  private val WholeRing = 1.0
}
