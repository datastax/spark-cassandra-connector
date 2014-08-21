package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import org.apache.spark.Logging

import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.util.CqlWhereParser._
import com.datastax.spark.connector.util._
import org.apache.cassandra.thrift
import org.apache.cassandra.thrift.Cassandra
import org.apache.spark.Partition
import org.apache.thrift.TApplicationException

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.reflect.macros.ParseException

/** Creates CassandraPartitions for given Cassandra table */
class CassandraRDDPartitioner[V, T <: Token[V]] (
                                                 connector: CassandraConnector,
                                                 tableDef: TableDef,
                                                 splitSize: Long)(
                                                 implicit
                                                 tokenFactory: TokenFactory[V, T])  extends Logging {

  type Token = com.datastax.spark.connector.rdd.partitioner.dht.Token[T]
  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[V, T]

  private val keyspaceName = tableDef.keyspaceName
  private val tableName = tableDef.tableName
  val pkNames = tableDef.partitionKey.map(_.columnName)

  private def unthriftify(tr: thrift.TokenRange): TokenRange = {
    val startToken = tokenFactory.fromString(tr.start_token)
    val endToken = tokenFactory.fromString(tr.end_token)
    val endpoints = tr.endpoints.map(InetAddress.getByName).toSet
    new TokenRange(startToken, endToken, endpoints, None)
  }

  private def describeRing(client: Cassandra.Iface): Seq[TokenRange] = {
    val ring =
      try {
        client.describe_local_ring(keyspaceName)
      }
      catch {
        case e: TApplicationException if e.getType == TApplicationException.UNKNOWN_METHOD =>
          client.describe_ring(keyspaceName)
        case e: java.lang.NoSuchMethodError =>
          client.describe_ring(keyspaceName)
      }
    ring.map(unthriftify)
  }

  private def quote(name: String) = "\"" + name + "\""

  private def splitsOf(tokenRanges: Iterable[TokenRange], splitter: TokenRangeSplitter[V, T]): Iterable[TokenRange] = {
    val parTokenRanges = tokenRanges.par
    parTokenRanges.tasksupport = new ForkJoinTaskSupport(CassandraRDDPartitioner.pool)
    (for (tokenRange <- parTokenRanges;
          split <- splitter.split(tokenRange, splitSize)) yield split).seq
  }

  private def splitToCqlClause(range: TokenRange): Iterable[CqlTokenRange] = {
    val startToken = tokenFactory.toString(range.start)
    val endToken = tokenFactory.toString(range.end)
    val pk = pkNames.map(quote).mkString(", ")

    if (range.end == tokenFactory.minToken)
      List(CqlTokenRange(s"token($pk) > $startToken"))
    else if (range.start == tokenFactory.minToken)
      List(CqlTokenRange(s"token($pk) <= $endToken"))
    else if (!range.isWrapAround)
      List(CqlTokenRange(s"token($pk) > $startToken AND token($pk) <= $endToken"))
    else
      List(
        CqlTokenRange(s"token($pk) > $startToken"),
        CqlTokenRange(s"token($pk) <= $endToken"))
  }

  /** This works only for numeric tokens */
  private def tokenCount(range: TokenRange): BigInt = {
    val start = BigInt(tokenFactory.toString(range.start))
    val end = BigInt(tokenFactory.toString(range.end))
    if (start < end)
      end - start
    else
      end - start + tokenFactory.totalTokenCount
  }

  /** Rows per token average is required for fast local range splitting.
    * Used only for Murmur3Partitioner and RandomPartitioner.  */
  private def estimateCassandraPartitionsPerToken(tokenRanges: Seq[TokenRange]): Double = {
    val random = new scala.util.Random(0)
    val tokenRangeSample = random.shuffle(tokenRanges).take(CassandraRDDPartitioner.TokenRangeSampleSize)
    val splitter = new ServerSideTokenRangeSplitter(connector, keyspaceName, tableName, tokenFactory)
    val splits = splitsOf(tokenRangeSample, splitter)
    val tokenCountSum = splits.map(tokenCount).sum
    val rowCountSum = splits.map(_.rowCount.get).sum
    rowCountSum.toDouble / tokenCountSum.toDouble
  }

  private def createSplitterFor(tokenRanges: Seq[TokenRange]): TokenRangeSplitter[V, T] = {
    tokenFactory.asInstanceOf[TokenFactory[_, _]] match {
      case TokenFactory.RandomPartitionerTokenFactory =>
        val partitionsPerToken = estimateCassandraPartitionsPerToken(tokenRanges)
        new RandomPartitionerTokenRangeSplitter(partitionsPerToken).asInstanceOf[TokenRangeSplitter[V, T]]
      case TokenFactory.Murmur3TokenFactory =>
        val partitionsPerToken = estimateCassandraPartitionsPerToken(tokenRanges)
        new Murmur3PartitionerTokenRangeSplitter(partitionsPerToken).asInstanceOf[TokenRangeSplitter[V, T]]
      case _ =>
        new ServerSideTokenRangeSplitter(connector, keyspaceName, tableName, tokenFactory)
    }
  }

  def containsPartitionKey(predicates: Seq[Predicate]) = {
    predicates.map(_.columnName).intersect(pkNames).nonEmpty
  }

  private def crossWhereClause(xs: Seq[CqlWhereClause], ys: Seq[CqlWhereClause]) =
    for {x <- xs; y <- ys} yield x and y

  private def partionKeBasedPartitions (whereClause: CqlWhereClause,
                                        predicates : Seq[Predicate],
                                        tokenRanges:Seq[TokenRange]): Array[Partition] = {
    var keyWhereClauses = Seq(CqlWhereClause(Seq[String](), Seq[Any]()))
    var otherWhereClause = CqlWhereClause(Seq[String](), Seq[Any]())
    val valIterator = whereClause.values.iterator
    for (predicate <- predicates) {
      //collect values for '?' for current predicate
      val qValues = predicate match {
        case InPredicateList(name, values) => values collect { case v: QParam => valIterator.next()}
        case InPredicate(_)
             | EqPredicate(_, QParam())
             | RangePredicate(_, _, QParam()) => Seq(valIterator.next())
        case _ => Seq()
      }

      // partition key predicates: cross join 'in' predicate into set of  '=' predicates to make paritions for parallel cql calls
      if (pkNames contains predicate.columnName) {
        keyWhereClauses = predicate match {
          case predicate: EqPredicate => keyWhereClauses.map(_
            and CqlWhereClause(Seq[String](predicate.toCqlString()), qValues))

          case predicate: InPredicate => {
            val eqWhereClauses = qValues.map(v => CqlWhereClause(Seq(quote(predicate.columnName) + " = ?"), Seq(v)))
            crossWhereClause(keyWhereClauses, eqWhereClauses)
          }

          case predicate: InPredicateList => {
            val valIterator = qValues.iterator
            val eqWhereClauses = predicate.values.map(_ match {
              case param: QParam => CqlWhereClause(Seq(quote(predicate.columnName) + " = ?"), Seq(valIterator.next))
              case param: Param => CqlWhereClause(Seq(quote(predicate.columnName) + " = " + param.toCqlString()), Seq())
            })
            crossWhereClause(keyWhereClauses, eqWhereClauses)
          }

          case param: RangePredicate => throw new RuntimeException("Only in and = operation accpeted for parition keys")

        }
        // not partition key predicates left as is
      } else {
        otherWhereClause = otherWhereClause.and(CqlWhereClause(Seq[String](predicate.toCqlString()), qValues))
      }
    }
    (for ((keyWhereClause, index) <- keyWhereClauses.zipWithIndex) yield {
      CassandraCustomPartition (index, tokenRanges.flatMap(_.endpoints).distinct,keyWhereClause and otherWhereClause)
    }).toArray
  }

  /** Computes Spark partitions of the given table. Called by [[CassandraRDD]]. */
  def partitions(whereClause: CqlWhereClause): Array[Partition] = {
    connector.withCassandraClientDo {
      client =>
        val tokenRanges = describeRing(client)
        val endpointCount = tokenRanges.map(_.endpoints).reduce(_ ++ _).size
        val splitter = createSplitterFor(tokenRanges)
        val splits = splitsOf(tokenRanges, splitter).toSeq
        val maxGroupSize = tokenRanges.size / endpointCount
        val clusterer = new TokenRangeClusterer[V, T](splitSize, maxGroupSize)
        val groups = clusterer.group(splits).toArray
        val predicates: Seq[Predicate] = whereClause.predicates.map(CqlWhereParser.predicates).fold(Seq[Predicate]()) {
          _ ++ _
        }
        try {
          if (containsPartitionKey(predicates)) {
            partionKeBasedPartitions (whereClause,predicates,tokenRanges)
          } else
            for ((group, index) <- groups.zipWithIndex) yield {
              val cqlPredicates = group.flatMap(splitToCqlClause)
              val endpoints = group.map(_.endpoints).reduce(_ intersect _)
              val rowCount = group.map(_.rowCount.get).sum
              CassandraRingPartition(index, endpoints, cqlPredicates, rowCount)
            }
        } catch {
          case x: Throwable => {
            logWarning("Partitioning error, one Partition for the query where created", x);
            Array(CassandraRingPartition(0, tokenRanges.flatMap(_.endpoints).distinct, List(CqlTokenRange("")), 0))
          }
        }
    }

  }

}

object CassandraRDDPartitioner {
  /** Affects how many concurrent threads are used to fetch split information from cassandra nodes, in `getPartitions`.
    * Does not affect how many Spark threads fetch data from Cassandra. */
  val MaxParallelism = 16

  /** How many token ranges to sample in order to estimate average number of rows per token */
  val TokenRangeSampleSize = 16

  private val pool: ForkJoinPool = new ForkJoinPool(MaxParallelism)
}
